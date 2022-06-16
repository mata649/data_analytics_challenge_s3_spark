import os
import requests

jars_urls = {
    'hadoop-aws-3.3.1.jar': 'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar',
    'aws-java-sdk-bundle-1.11.901.jar': 'https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar',
    'postgresql-42.4.0.jar': 'https://jdbc.postgresql.org/download/postgresql-42.4.0.jar',
}

if __name__ == '__main__':
    jars_folder = os.path.join(os.getenv('SPARK_HOME'), 'jars')
    for jar_name, jar_url in jars_urls.items():
        with open(os.path.join(jars_folder, jar_name), 'wb') as f:
            jar = requests.get(jar_url)
            f.write(jar.content)
