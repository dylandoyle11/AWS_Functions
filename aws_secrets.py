import boto3
import json


class SecretService:

    def __init__(self, service='secretsmanager'):
        """Pass 'secretsmanager' to use AWS Secrets Manager, or 'ssm' to use AWS Systems Manager (Parameter Store)"""
        assert service in ['secretsmanager', 'ssm'], "ERROR: Must pass either 'secretsmanager' or 'ssm' to service parameter"
        self.region_name = 'ca-central-1'
        # Create a Secrets Manager client
        self.session = boto3.session.Session()
        self.client = self.session.client(
            service_name=service,
            region_name=self.region_name
        )

    def get_secretsmanager_secret(self, secret_name):
        """Get secret value stored in AWS Secrets Manager, using specified secret_name and region_name."""

        get_secret_value_response = self.client.get_secret_value(
            SecretId=secret_name
        )
        secret_json = json.loads(get_secret_value_response['SecretString'])
        secret_value = secret_json[secret_name]

        return secret_value

    def get_secretsmanager_db_creds(self, secret_name):
        """Get Db creds value stored in AWS Secrets Manager, using specified secret_name and region_name.
        Returns dict
        {'username': '...',
        'password': '...',
        'engine': '...',
        'host': '...',
        'port': '...',
        'dbname': '...'}
        """

        get_secret_value_response = self.client.get_secret_value(
            SecretId=secret_name
        )
        db_creds_dict = json.loads(get_secret_value_response['SecretString'])

        return db_creds_dict

    def get_paramstore_db_creds(self, secret_name):
        """Get Db creds value stored in AWS Systems Manager, using specified secret_name and region_name.
        Returns dict
        {'username': '...',
        'password': '...',
        'engine': '...',
        'host': '...',
        'port': '...',
        'dbname': '...'}
        """

        get_secret_value_response = self.get_paramstore_secret(secret_name)
        db_creds_dict = json.loads(get_secret_value_response)

        return db_creds_dict

    def get_paramstore_secret(self, secret_name):
        """Get SecureString value stored in AWS Systems Manager Parameter Store, using specified secret_name and
        region_name."""

        secret_value_response = self.client.get_parameter(
            Name=secret_name, WithDecryption=True
        )
        return secret_value_response['Parameter']['Value']
