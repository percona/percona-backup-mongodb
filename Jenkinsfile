def skipBranchBulds = true
if ( env.CHANGE_URL ) {
    skipBranchBulds = false
}

TestsReport = '| Test name  | Status |\\r\\n| ------------- | ------------- |'
testsReportMap  = [:]

void makeReport() {
    for ( test in testsReportMap ) {
        TestsReport = TestsReport + "\\r\\n| ${test.key} | ${test.value} |"
    }
}

pipeline {
    environment {
        AUTHOR_NAME  = sh(script: "echo ${CHANGE_AUTHOR_EMAIL} | awk -F'@' '{print \$1}'", , returnStdout: true).trim()
    }
    agent {
        label 'micro-amazon'
    }
    stages {
        stage('Run tests for PBM') {
            when {
                expression {
                    !skipBranchBulds
                }
            }
            parallel {
                stage('PBM tests mongodb 3.6') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        script {
                            if ( AUTHOR_NAME == 'null' )  {
                                 AUTHOR_NAME = sh(script: "git show -s --pretty=%ae | awk -F'@' '{print \$1}'", , returnStdout: true).trim()
                            }
                            testsReportMap['mongodb 3.6'] = 'failed'
                        }
                        withCredentials([file(credentialsId: 'PBM-AWS-S3', variable: 'PBM_AWS_S3_YML'), file(credentialsId: 'PBM-GCS-S3', variable: 'PBM_GCS_S3_YML')]) {
                            sh '''
                                sudo curl -L "https://github.com/docker/compose/releases/download/1.25.3/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
                                sudo chmod +x /usr/local/bin/docker-compose

                                cp $PBM_AWS_S3_YML ./e2e-tests/docker/conf/aws.yaml
                                cp $PBM_GCS_S3_YML ./e2e-tests/docker/conf/gcs.yaml

                                chmod 664 ./e2e-tests/docker/conf/aws.yaml
                                chmod 664 ./e2e-tests/docker/conf/gcs.yaml

                                docker-compose -f ./e2e-tests/docker/docker-compose.yaml build
                                openssl rand -base64 756 > ./e2e-tests/docker/keyFile
                                sudo chown 1001:1001 ./e2e-tests/docker/keyFile
                                sudo chmod 400 ./e2e-tests/docker/keyFile
                            '''
                        }
                        sh '''
                            export MONGODB_VERSION='3.6'
                            exit 1
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml up --quiet-pull --no-color -d

                            export COMPOSE_INTERACTIVE_NO_CLI=1
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T cfg01 /opt/start.sh
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T rs101 /opt/start.sh
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T rs201 /opt/start.sh
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T mongos mongo mongodb://dba:test1234@localhost /opt/mongos_init.js

                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml stop \
                                 agent-cfg01 agent-cfg02 agent-cfg03  agent-rs101 agent-rs102 agent-rs103 agent-rs201 agent-rs202 agent-rs203
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml start \
                                 agent-cfg01 agent-cfg02 agent-cfg03  agent-rs101 agent-rs102 agent-rs103 agent-rs201 agent-rs202 agent-rs203

                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml start tests
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml logs -f --no-color tests
                            EXIT_CODE=$(docker-compose -f ./e2e-tests/docker/docker-compose.yaml ps -q tests | xargs docker inspect -f '{{ .State.ExitCode }}')
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml ps
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml down
                            
                            exit $EXIT_CODE
                        '''
                        script {
                            testsReportMap['mongodb 3.6'] = 'passed'
                        }
                    }
                }
                stage('PBM tests mongodb 4.0') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        script {
                            testsReportMap['mongodb 4.0'] = 'failed'
                        }
                        withCredentials([file(credentialsId: 'PBM-AWS-S3', variable: 'PBM_AWS_S3_YML'), file(credentialsId: 'PBM-GCS-S3', variable: 'PBM_GCS_S3_YML')]) {
                            sh '''
                                sudo curl -L "https://github.com/docker/compose/releases/download/1.25.3/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
                                sudo chmod +x /usr/local/bin/docker-compose

                                cp $PBM_AWS_S3_YML ./e2e-tests/docker/conf/aws.yaml
                                cp $PBM_GCS_S3_YML ./e2e-tests/docker/conf/gcs.yaml

                                chmod 664 ./e2e-tests/docker/conf/aws.yaml
                                chmod 664 ./e2e-tests/docker/conf/gcs.yaml

                                docker-compose -f ./e2e-tests/docker/docker-compose.yaml build
                                openssl rand -base64 756 > ./e2e-tests/docker/keyFile
                                sudo chown 1001:1001 ./e2e-tests/docker/keyFile
                                sudo chmod 400 ./e2e-tests/docker/keyFile
                            '''
                        }
                        sh '''
                            export MONGODB_VERSION='4.0'
                            exit 1
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml up --quiet-pull --no-color -d

                            export COMPOSE_INTERACTIVE_NO_CLI=1
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T cfg01 /opt/start.sh
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T rs101 /opt/start.sh
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T rs201 /opt/start.sh
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T mongos mongo mongodb://dba:test1234@localhost /opt/mongos_init.js

                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml stop \
                                 agent-cfg01 agent-cfg02 agent-cfg03  agent-rs101 agent-rs102 agent-rs103 agent-rs201 agent-rs202 agent-rs203
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml start \
                                 agent-cfg01 agent-cfg02 agent-cfg03  agent-rs101 agent-rs102 agent-rs103 agent-rs201 agent-rs202 agent-rs203

                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml start tests
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml logs -f --no-color tests
                            EXIT_CODE=$(docker-compose -f ./e2e-tests/docker/docker-compose.yaml ps -q tests | xargs docker inspect -f '{{ .State.ExitCode }}')
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml ps
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml down
                            
                            exit $EXIT_CODE
                        '''
                        script {
                            testsReportMap['mongodb 4.0'] = 'passed'
                        }
                    }
                }
                stage('PBM tests mongodb 4.2') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        script {
                            testsReportMap['mongodb 4.2'] = 'failed'
                        }
                        withCredentials([file(credentialsId: 'PBM-AWS-S3', variable: 'PBM_AWS_S3_YML'), file(credentialsId: 'PBM-GCS-S3', variable: 'PBM_GCS_S3_YML')]) {
                            sh '''
                                sudo curl -L "https://github.com/docker/compose/releases/download/1.25.3/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
                                sudo chmod +x /usr/local/bin/docker-compose

                                cp $PBM_AWS_S3_YML ./e2e-tests/docker/conf/aws.yaml
                                cp $PBM_GCS_S3_YML ./e2e-tests/docker/conf/gcs.yaml

                                chmod 664 ./e2e-tests/docker/conf/aws.yaml
                                chmod 664 ./e2e-tests/docker/conf/gcs.yaml

                                docker-compose -f ./e2e-tests/docker/docker-compose.yaml build
                                openssl rand -base64 756 > ./e2e-tests/docker/keyFile
                                sudo chown 1001:1001 ./e2e-tests/docker/keyFile
                                sudo chmod 400 ./e2e-tests/docker/keyFile
                            '''
                        }
                        sh '''
                            export MONGODB_VERSION='4.2'
                            exit 1
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml up --quiet-pull --no-color -d

                            export COMPOSE_INTERACTIVE_NO_CLI=1
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T cfg01 /opt/start.sh
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T rs101 /opt/start.sh
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T rs201 /opt/start.sh
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml exec -T mongos mongo mongodb://dba:test1234@localhost /opt/mongos_init.js

                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml stop \
                                 agent-cfg01 agent-cfg02 agent-cfg03  agent-rs101 agent-rs102 agent-rs103 agent-rs201 agent-rs202 agent-rs203
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml start \
                                 agent-cfg01 agent-cfg02 agent-cfg03  agent-rs101 agent-rs102 agent-rs103 agent-rs201 agent-rs202 agent-rs203

                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml start tests
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml logs -f --no-color tests
                            EXIT_CODE=$(docker-compose -f ./e2e-tests/docker/docker-compose.yaml ps -q tests | xargs docker inspect -f '{{ .State.ExitCode }}')
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml ps
                            docker-compose -f ./e2e-tests/docker/docker-compose.yaml down
 
                            exit $EXIT_CODE
                        '''
                        script {
                            testsReportMap['mongodb 4.2'] = 'passed'
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            script {
                if (env.CHANGE_URL) {
                    withCredentials([string(credentialsId: 'GITHUB_API_TOKEN', variable: 'GITHUB_API_TOKEN')]) {
                        makeReport()
                        sh """
                            curl -v -X POST \
                                -H "Authorization: token ${GITHUB_API_TOKEN}" \
                                -d "{\\"body\\":\\"${TestsReport}\\"}" \
                                "https://api.github.com/repos/\$(echo $CHANGE_URL | cut -d '/' -f 4-5)/issues/${CHANGE_ID}/comments"
                        """
                    }
                }
            }
            sh '''
                sudo docker rmi -f \$(sudo docker images -q) || true
                sudo rm -rf ./*
            '''
            deleteDir()
        }
        failure {
            script {
                slackSend channel: '#cloud-dev-ci', color: '#FF0000', message: "[${JOB_NAME}]: build ${currentBuild.result}, ${BUILD_URL} owner: @${AUTHOR_NAME}"
            }
        }
    }
}
