pipeline {
agent any


    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }

    environment {
        IMAGE_NAME = "python-analytics"
    }

    stages {

        stage('Checkout') {
            steps {
                git branch: 'main',
                url: 'https://github.com/puj16/electrical-Usage-Analytic.git'
            }
        }

        stage('Build Docker Image') {
            steps {
                sh '''
                    docker build -t ${IMAGE_NAME}:latest .
                '''
            }
        }

        stage('Deploy Container') {
            steps {
                withCredentials([file(credentialsId: 'python-analytics-env', variable: 'ENV_FILE')]) {
                    sh '''
                        cp $ENV_FILE .env

                        docker compose down || true
                        docker compose up -d
                    '''
                }
            }
        }

        stage('Cleanup Old Images') {
            steps {
                sh '''
                    docker image prune -f
                '''
            }
        }

    }

    post {
        success {
            echo 'Deploy berhasil ✅'
        }

        failure {
            echo 'Deploy gagal ❌'
        }
    }


}
