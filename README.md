# guidance-on-automating-the-background-check-for-reporting-and-audit-compliance
guidance-on-automating-the-background-check-for-reporting-and-audit-compliance


 Set up

1. Set up your Cloud9 instance for CodeCommit (optional) - https://docs.aws.amazon.com/codecommit/latest/userguide/setting-up-ide-c9.html
2. Install Typescript - npm -g install typescript 
3. Bootstrap the project as one time activity, to deploy stacks with the AWS CDK requires dedicated Amazon S3 buckets and other containers to be available to AWS CloudFormation during deployment – 
cdk bootstrap aws://ACCOUNT-NUMBER/REGION
4. clone the project from source commit repo 
5. Install the required node modules by running below command on project root 
npm install
6. Create sharedLayer directory on project root. Install required python packages under sharedLayer/python directory path as below - 
pip install -r requirements.txt --target=./sharedLayer/python/
12.	To deploy the solution run,
cdk synth 
cdk deploy 


