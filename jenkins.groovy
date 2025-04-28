pipeline {
    agent any

    environment {
        AWS_REGION = 'eu-west-1'
        INSTANCE_ID = 'i-0c3539654b8242b41'
        WORKSPACE_DIR = '/var/lib/jenkins/workspace/seed-eu-github-stg-branch-NonProd-deploy'
    }

    triggers {
        // Trigger this job after the 'seed-eu-github-stg-branch-NonProd-deploy' success
        upstream(upstreamProjects: 'seed-eu-github-stg-branch-NonProd-deploy', threshold: hudson.model.Result.SUCCESS)
    }

    stages {
        stage('Checkout') {
            steps {
                git(
                    credentialsId: '7222e4cd-f32a-4576-bc76-0bf4f7357e42',
                    branch: 'stg',
                    url: 'https://github.com/SyscoCorporation/seed-eu.git'
                )
            }
        }

        stage('Get Commit Details') {
            steps {
                script {
                    def modified = sh(
                        script: "git diff --name-only HEAD~1 HEAD",
                        returnStdout: true
                    ).trim()

                    def added = sh(
                        script: "git diff --name-only --diff-filter=A HEAD~1 HEAD",
                        returnStdout: true
                    ).trim()

                    def commit_message = sh(
                        script: "git log -1 --pretty=%B",
                        returnStdout: true
                    ).trim()

                    echo "Modified files: ${modified}"
                    echo "Added files: ${added}"
                    echo "Commit message: ${commit_message}"

                    // Process added and modified file lists
                    def processYamlFiles = { input ->
                        input.replaceAll(/\.yaml/, '.yaml,')    // Append a comma after each .yaml
                             .replaceAll(',\\s*$', '')           // Remove trailing comma
                    }

                    def processedAdded = processYamlFiles("${added}")
                    def processedModified = processYamlFiles("${modified}")

                    // Combine the processed strings
                    def dp_list = "${processedAdded}, ${processedModified}".replaceAll(' ,', ',').replaceAll(', $', '')

                    echo "Processed Added Files: ${processedAdded}"
                    echo "Processed Modified Files: ${processedModified}"
                    echo "dp_list: ${dp_list}"

                    // Set dp_list to environment variable if needed later
                    env.DP_LIST = dp_list
                }
            }
        }

        stage('AWS Checks - Parallel Execution') {
            when {
                expression {
                    env.DP_LIST != null && env.DP_LIST.contains("data-checks/")
                }
            }
            parallel {
                stage('Sync Data Checks to S3') {
                    steps {
                        script {
                            echo "Changes to data checks folder detected"
                            def s3_bucket = sh(
                                script: """
                                    aws ssm get-parameter --name /SEED/EU/S3/BUCKET/WORM-AUDIT \\
                                    --query 'Parameter.Value' --output text | tr -d '\"'
                                """,
                                returnStdout: true
                            ).trim()

                            echo "Syncing data-checks folder to S3 bucket: ${s3_bucket}"

                            sh """
                              aws s3 sync data-checks/ "s3://${s3_bucket}/data-checks/" \\
                              --exact-timestamps --delete --exclude "*.gitkeep" --exclude "*.log"
                            """

                            def exitCode = sh(script: "echo \$?", returnStdout: true).trim()
                            if (exitCode != "0") {
                                error("S3 sync command failed.")
                            }
                            echo "Data-checks folder synced successfully."
                        }
                    }
                }

                stage('Check and Start EC2 Instance') {
                    steps {
                        script {
                            checkAndStartEC2(env.INSTANCE_ID)
                        }
                    }
                }
            }
        }

        stage('Run DQ Automation Framework') {
            when {
                expression { env.DP_LIST != null && env.DP_LIST.contains("data-checks/") }
            }
            agent {
                label 'QA'
            }
            steps {
                script {
                    echo "Setting DP_LIST as an environment variable"


                    // Run the container in detached mode
                    def containerId = sh(
                        script: """
                            podman run --network host --cgroup-manager=cgroupfs -d --user appuser
                            test_auto_mvp_1:latest tail -f /dev/null", returnStdout: true).trim()
                        """
                    sh "echo 'Container started with ID: ${containerId}'"

                    // Execute the test inside the container and capture the exit code

                    // aws configure set region eu-west-1 && \\
                    // source /root/.cache/pypoetry/virtualenvs/seed-qe-automation-framework-*/bin/activate && \\

                    def exitCode = sh(
                        script: """
                            podman exec ${containerId} bash -c '
                            python3 main.py --run_mode cicd \\
                            --test_env stg \\
                            --file_names "${env.DP_LIST}"
                            '
                        """,
                        returnStdout: true
                    ).trim()

                    // Stop the container
                    sh "podman stop --time 5 ${containerId}"
                    sh """
                        while podman ps -a | grep -q ${containerId}; do
                            echo 'Waiting for container ${containerId} to stop...'
                            sleep 1
                        done
                    """
                    echo "Container stopped successfully."

                    // Remove the container
                    sh "podman rm ${containerId}"
                    echo "Container removed successfully."

                    echo "+++++++++++++++++++++++++++++++++++."
                    echo("Pytest failed with exit code ${exitCode}")
                    echo "+++++++++++++++++++++++++++++++++++."

                    // Extract only the "Test session finished with exit status X" line
                    def match = (exitCode =~ /Test session finished with exit status \d+/)

                    // Echo the extracted exit status line
                    if (match) {
                        echo "Extracted exit status: ${match[0]} Marking Jenkins job as success."
                    } else {
                        echo "Could not find exit status in logs."
                        error("Pytest failed with exit code ${match[0]} Marking Jenkins job as failed")
                    }
                }
            }
        }
    }
}

// Function to check and start EC2 instance
def checkAndStartEC2(instanceId) {
    def instance_state = sh(
        script: """
            aws ec2 describe-instances --instance-ids ${instanceId} \\
            --query 'Reservations[*].Instances[*].State.Name' \\
            --output text
        """,
        returnStdout: true
    ).trim()

    echo "Initial instance state: ${instance_state}"

    if (instance_state == "stopped") {
        echo "EC2 instance is stopped. Starting the instance..."
        sh "aws ec2 start-instances --instance-ids ${instanceId}"
        waitForInstanceRunning(instanceId)
    } else if (instance_state == "running") {
        echo "EC2 instance is already running."
    } else if (instance_state == "pending") {
        echo "EC2 instance is pending. Waiting for it to become running..."
        waitForInstanceRunning(instanceId)
    } else if (instance_state == "stopping") {
        echo "EC2 instance is stopping. Waiting for it to become stopped before starting..."
        waitForStopped(instanceId)
        echo "Starting EC2 instance now that it is stopped..."
        sh "aws ec2 start-instances --instance-ids ${instanceId}"
        waitForInstanceRunning(instanceId)
    } else {
        error("EC2 instance is in an unexpected state: ${instance_state}. Cannot start.")
    }

    // Wait for health checks to pass, with additional restart logic if needed
    waitForInstanceStatus(instanceId)
}

// Function to wait for the instance to be in the running state
def waitForInstanceRunning(instanceId) {
    while (true) {
        sleep(10)
        def instance_state = sh(
            script: """
                aws ec2 describe-instances --instance-ids ${instanceId} \\
                --query 'Reservations[*].Instances[*].State.Name' \\
                --output text
            """,
            returnStdout: true
        ).trim()

        echo "Current instance state: ${instance_state}"

        if (instance_state == "running") {
            echo "EC2 instance is now running."
            break
        }
    }
}

// Function to wait for the instance to be stopped
def waitForStopped(instanceId) {
    while (true) {
        sleep(10)
        def instance_state = sh(
            script: """
                aws ec2 describe-instances --instance-ids ${instanceId} \\
                --query 'Reservations[*].Instances[*].State.Name' \\
                --output text
            """,
            returnStdout: true
        ).trim()
        echo "Waiting for instance to stop. Current state: ${instance_state}"
        if (instance_state == "stopped") {
            echo "Instance is now stopped."
            break
        }
    }
}

// Function to wait for EC2 instance status checks to pass
// with logic to restart if the instance unexpectedly stops.
def waitForInstanceStatus(instanceId) {
    int maxAttempts = 12  // Approximately 2 minutes (12 * 10 sec)
    for (int i = 0; i < maxAttempts; i++) {
        sleep(10)
        def instance_status = sh(
            script: """
                aws ec2 describe-instance-status --instance-ids ${instanceId} \\
                --query 'InstanceStatuses[*].InstanceStatus.Status' \\
                --output text
            """,
            returnStdout: true
        ).trim()

        def system_status = sh(
            script: """
                aws ec2 describe-instance-status --instance-ids ${instanceId} \\
                --query 'InstanceStatuses[*].SystemStatus.Status' \\
                --output text
            """,
            returnStdout: true
        ).trim()

        echo "Current instance status: ${instance_status}, system status: ${system_status}"

        if (instance_status == "ok" && system_status == "ok") {
            echo "All status checks passed. Proceeding with commands."
            return
        }
        
        // Re-check the overall instance state. If it's stopped unexpectedly attempt a restart
        def current_state = sh(
            script: """
                aws ec2 describe-instances --instance-ids ${instanceId} \\
                --query 'Reservations[*].Instances[*].State.Name' \\
                --output text
            """,
            returnStdout: true
        ).trim()

        if (current_state == "stopped") {
            echo "Instance state is 'stopped' while waiting for status checks. Attempting to restart..."
            checkAndStartEC2(instanceId)
            return
        }
    }
    error("Instance status did not become 'ok' after waiting.")
}