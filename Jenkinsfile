pipeline {

    agent any

    tools {
        maven 'Maven 3'
        jdk 'Java 8'
    }

    stages {
        stage ('Build') {
            steps {
                sh 'mvn clean package'
            }
        }

        stage ('Deploy') {
            when {
                branch "master"
            }
            steps {
                sh 'mvn source:jar deploy -DskipTests'
            }
        }
    }

    post {
        always {
            deleteDir()
        }
    }
}