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

void runTest(String TEST_NAME, String TEST_SCRIPT, String MONGO_VERSION) {
    def mkey = "$TEST_NAME mongodb $MONGO_VERSION"

    testsReportMap[mkey] = 'failed'

    // we don't have psmdb 5.0 images for the time being
    def mongo_img = 'percona/percona-server-mongodb'
    if ( MONGO_VERSION == '5.0' )  {
        mongo_img = 'mongo'
    }

    sh """
        set -o xtrace
        export MONGODB_IMAGE=${mongo_img}
        export MONGODB_VERSION=${MONGO_VERSION}
        export PBM_TESTS_NO_BUILD=true
        ./e2e-tests/${TEST_SCRIPT}
    """

    testsReportMap[mkey] = 'passed'
}

void prepareCluster(String CLUSTER_TYPE, String TEST_TYPE) {
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

            docker-compose -f ./e2e-tests/docker/${compose} build
            openssl rand -base64 756 > ./e2e-tests/docker/keyFile
            sudo chown 1001:1001 ./e2e-tests/docker/keyFile
            sudo chmod 400 ./e2e-tests/docker/keyFile
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
                stage('Restore on new cluster 3.6') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        script {
                            if ( AUTHOR_NAME == 'null' )  {
                                 AUTHOR_NAME = sh(script: "git show -s --pretty=%ae | awk -F'@' '{print \$1}'", , returnStdout: true).trim()
                            }
                        }

                        prepareCluster('sharded', '36')
                        runTest('Restore on new cluster', 'run-new-cluster', '3.6')
                    }
                }
                stage('Restore on new cluster 4.0') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('sharded', '40-newc')
                        runTest('Restore on new cluster', 'run-new-cluster', '4.0')
                    }
                }
                stage('Restore on new cluster 4.2') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('sharded', '42-newc')
                        runTest('Restore on new cluster', 'run-new-cluster', '4.2')
                    }
                }
                stage('Restore on new cluster 4.4') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('sharded', '44-newc')
                        runTest('Restore on new cluster', 'run-new-cluster', '4.4')
                    }
                }
                stage('Restore on new cluster 5.0') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('sharded', '50-newc')
                        runTest('Restore on new cluster', 'run-new-cluster', '5.0')
                    }
                }

                stage('Sharded cluster 3.6') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '36-shrd')
                        runTest('Sharded cluster', 'run-sharded', '3.6')
                    }
                }
                stage('Sharded cluster 4.0') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '40-shrd')
                        runTest('Sharded cluster', 'run-sharded', '4.0')
                    }
                }
                stage('Sharded cluster 4.2') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '42-shrd')
                        runTest('Sharded cluster', 'run-sharded', '4.2')
                    }
                }
                stage('Sharded cluster 4.4') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '44-shrd')
                        runTest('Sharded cluster', 'run-sharded', '4.4')
                    }
                }
                stage('Sharded cluster 5.0') {
                    agent {
                        label 'docker-32gb'
                    }
                    steps {
                        prepareCluster('sharded', '50-shrd')
                        runTest('Sharded cluster', 'run-sharded', '5.0')
                    }
                }

                stage('Non-sharded replicaset 3.6') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '36-rs')
                        runTest('Non-sharded replicaset', 'run-rs', '3.6')
                    }
                }
                stage('Non-sharded replicaset 4.0') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '40-rs')
                        runTest('Non-sharded replicaset', 'run-rs', '4.0')
                    }
                }
                stage('Non-sharded replicaset 4.2') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '42-rs')
                        runTest('Non-sharded replicaset', 'run-rs', '4.2')
                    }
                }
                stage('Non-sharded replicaset 4.4') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '44-rs')
                        runTest('Non-sharded replicaset', 'run-rs', '4.4')
                    }
                }
                stage('Non-sharded replicaset 5.0') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('rs', '50-rs')
                        runTest('Non-sharded replicaset', 'run-rs', '5.0')
                    }
                }

                stage('Single-node replicaset 3.6') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '36-single')
                        runTest('Single-node replicaset', 'run-single', '3.6')
                    }
                }
                stage('Single-node replicaset 4.0') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '40-single')
                        runTest('Single-node replicaset', 'run-single', '4.0')
                    }
                }
                stage('Single-node replicaset 4.2') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '42-single')
                        runTest('Single-node replicaset', 'run-single', '4.2')
                    }
                }
                stage('Single-node replicaset 4.4') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '44-single')
                        runTest('Single-node replicaset', 'run-single', '4.4')
                    }
                }
                stage('Single-node replicaset 5.0') {
                    agent {
                        label 'docker'
                    }
                    steps {
                        prepareCluster('single', '50-single')
                        runTest('Single-node replicaset', 'run-single', '5.0')
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
