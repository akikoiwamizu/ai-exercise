##########################################
# AWS CLI COMMANDS USED TO COMPLETE Q1:
##########################################

# Setup credentials and S3 access:
aws configure --profile ai-test

# Confirm S3 bucket access:
aws s3 ls

# Copy files to S3 bucket:
aws s3 cp files/oh11.csv s3://ai-hotel/
