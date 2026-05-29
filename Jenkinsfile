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
                sh '''
                    docker compose down || true
                    docker compose up -d
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
