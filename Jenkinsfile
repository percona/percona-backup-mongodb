def skipBranchBulds = true
if ( env.CHANGE_URL ) {
    skipBranchBulds = false
}

TestsReport = '| Test name  | Logical | Physical |\\r\\n| ------------- | ------------- | ------------- |'
testsReportMap  = [:]

void makeReport() {
    for ( test in testsReportMap ) {
        TestsReport = TestsReport + "\\r\\n| ${test.key} | ${test.value.logical} | ${test.value.physical} |"
    }
}

void runTest(String TEST_NAME, String TEST_SCRIPT, String MONGO_VERSION, String BCP_TYPE) {
    def mkey = "$TEST_NAME psmdb $MONGO_VERSION"

    if (!testsReportMap[mkey]) {
        testsReportMap[mkey]=[:]
        testsReportMap[mkey]['logical'] = '-'
        testsReportMap[mkey]['physical'] = '-'
    }
    testsReportMap[mkey][BCP_TYPE] = 'failed'

    sh """
        chmod 777 -R e2e-tests/docker/backups
        export MONGODB_VERSION=${MONGO_VERSION}
        export TESTS_BCP_TYPE=${BCP_TYPE}
        ./e2e-tests/${TEST_SCRIPT}
    """

    testsReportMap[mkey][BCP_TYPE] = 'passed'
}

void prepareCluster(String CLUSTER_TYPE, String TEST_TYPE, String MONGO_VERSION) {
    def compose = 'docker-compose.yaml'

    switch(CLUSTER_TYPE) {            
        case 'rs': 
            compose = 'docker-compose-rs.yaml'
            break
        case 'single': 
            compose = 'docker-compose-single.yaml'
            break
        default: 
            compose = 'docker-compose.yaml'
            break 
   }

    withCredentials([file(credentialsId: 'PBM-AWS-S3', variable: 'PBM_AWS_S3_YML'), file(credentialsId: 'PBM-GCS-S3', variable: 'PBM_GCS_S3_YML'), file(credentialsId: 'PBM-AZURE', variable: 'PBM_AZURE_YML')]) {
        sh """
            sudo curl -L "https://github.com/docker/compose/releases/download/1.25.3/docker-compose-\$(uname -s)-\$(uname -m)" -o /usr/local/bin/docker-compose
            sudo chmod +x /usr/local/bin/docker-compose

            cp $PBM_AWS_S3_YML ./e2e-tests/docker/conf/aws.yaml
            cp $PBM_GCS_S3_YML ./e2e-tests/docker/conf/gcs.yaml
            cp $PBM_AZURE_YML ./e2e-tests/docker/conf/azure.yaml
            sed -i s:pbme2etest:pbme2etest-${TEST_TYPE}:g ./e2e-tests/docker/conf/aws.yaml
            sed -i s:pbme2etest:pbme2etest-${TEST_TYPE}:g ./e2e-tests/docker/conf/gcs.yaml
            sed -i s:pbme2etest:pbme2etest-${TEST_TYPE}:g ./e2e-tests/docker/conf/azure.yaml

            chmod 664 ./e2e-tests/docker/conf/aws.yaml
            chmod 664 ./e2e-tests/docker/conf/gcs.yaml
            chmod 664 ./e2e-tests/docker/conf/azure.yaml

            openssl rand -base64 756 > ./e2e-tests/docker/keyFile
        """
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
                stage('New cluster 4.2 logical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        script {
                            if ( AUTHOR_NAME == 'null' )  {
                                 AUTHOR_NAME = sh(script: "git show -s --pretty=%ae | awk -F'@' '{print \$1}'", , returnStdout: true).trim()
                            }
                        }

                        prepareCluster('sharded', '42-newc-logic', '4.2')
                        runTest('New cluster', 'run-new-cluster', '4.2', 'logical')
                    }
                }
                stage('New cluster 4.4 logical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('sharded', '44-newc-logic', '4.4')
                        runTest('New cluster', 'run-new-cluster', '4.4', 'logical')
                    }
                }
                stage('New cluster 5.0 logical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('sharded', '50-newc-logic', '5.0')
                        runTest('New cluster', 'run-new-cluster', '5.0', 'logical')
                    }
                }

                stage('Sharded 4.2 logical') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '42-shrd-logic', '4.2')
                        runTest('Sharded', 'run-sharded', '4.2', 'logical')
                    }
                }
                stage('Sharded 4.4 logical') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '44-shrd-logic', '4.4')
                        runTest('Sharded', 'run-sharded', '4.4', 'logical')
                    }
                }
                stage('Sharded 5.0 logical') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '50-shrd-logic', '5.0')
                        runTest('Sharded', 'run-sharded', '5.0', 'logical')
                    }
                }

                stage('Non-sharded 4.2 logical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '42-rs-logic', '4.2')
                        runTest('Non-sharded', 'run-rs', '4.2', 'logical')
                    }
                }
                stage('Non-sharded 4.4 logical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '44-rs-logic', '4.4')
                        runTest('Non-sharded', 'run-rs', '4.4', 'logical')
                    }
                }
                stage('Non-sharded 5.0 logical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '50-rs-logic', '5.0')
                        runTest('Non-sharded', 'run-rs', '5.0', 'logical')
                    }
                }

                stage('Single-node 4.2 logical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '42-single-logic', '4.2')
                        runTest('Single-node', 'run-single', '4.2', 'logical')
                    }
                }
                stage('Single-node 4.4 logical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '44-single-logic', '4.4')
                        runTest('Single-node', 'run-single', '4.4', 'logical')
                    }
                }
                stage('Single-node 5.0 logical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '50-single-logic', '5.0')
                        runTest('Single-node', 'run-single', '5.0', 'logical')
                    }
                }

                stage('Sharded 4.2 physical') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '42-shrd-phys', '4.2')
                        runTest('Sharded', 'run-sharded', '4.2', 'physical')
                    }
                }
                stage('Sharded 4.4 physical') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '44-shrd-phys', '4.4')
                        runTest('Sharded', 'run-sharded', '4.4', 'physical')
                    }
                }
                stage('Sharded 5.0 physical') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '50-shrd-phys', '5.0')
                        runTest('Sharded', 'run-sharded', '5.0', 'physical')
                    }
                }

                stage('Non-sharded 4.2 physical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '42-rs-phys', '4.2')
                        runTest('Non-sharded', 'run-rs', '4.2', 'physical')
                    }
                }
                stage('Non-sharded 4.4 physical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '44-rs-phys', '4.4')
                        runTest('Non-sharded', 'run-rs', '4.4', 'physical')
                    }
                }
                stage('Non-sharded 5.0 physical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '50-rs-phys', '5.0')
                        runTest('Non-sharded', 'run-rs', '5.0', 'physical')
                    }
                }

                stage('Single-node 4.2 physical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '42-single-phys', '4.2')
                        runTest('Single-node', 'run-single', '4.2', 'physical')
                    }
                }
                stage('Single-node 4.4 physical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '44-single-phys', '4.4')
                        runTest('Single-node', 'run-single', '4.4', 'physical')
                    }
                }
                stage('Single-node 5.0 physical') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '50-single-phys', '5.0')
                        runTest('Single-node', 'run-single', '5.0', 'physical')
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
