from AWS_connector import aws_instantiate

con = aws_instantiate(credential = ['AKIAI6S4IIWX62NTXIYQ',
 'CTdVw+mV+ajPEYUTJzvga52jaKZABnSqKGOEvhUZ'], region = 'us-east-1')

con.load_credential()
client= con.client_boto()

client['s3']
