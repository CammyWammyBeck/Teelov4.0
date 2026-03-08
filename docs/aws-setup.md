# AWS Setup For Teelo Model Storage

This guide configures S3-backed model artifact storage for the Teelo ML pipeline.

## 1. Create an AWS account

1. Go to `https://aws.amazon.com/` and create an account if you do not already have one.
2. Complete billing and phone verification.
3. Sign in to the AWS Console.

## 2. Create the S3 bucket

1. Open the S3 service in AWS Console.
2. Click `Create bucket`.
3. Set bucket name to `teelo-models`.
4. Choose a region close to your deployment.
5. Leave default settings unless your security/compliance policy requires changes.
6. Create the bucket.

## 3. Create an IAM user with bucket-scoped S3 access

1. Open IAM in AWS Console.
2. Create a new user (for example `teelo-model-storage`).
3. Enable `Access key - Programmatic access`.
4. Attach an inline policy scoped to `teelo-models` only:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::teelo-models"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::teelo-models/models/*"
    }
  ]
}
```

## 4. Get access keys

1. In IAM, open the user you created.
2. Create an access key.
3. Save both values securely:
   - `Access key ID`
   - `Secret access key`

## 5. Add keys to `.env`

Add these values to your project `.env` file:

```bash
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
S3_MODEL_BUCKET=teelo-models
```

## 6. Test Teelo S3 model access

Run:

```bash
teelo model list
```

Expected result:
- If empty bucket: `No models found in S3 bucket.`
- If artifacts exist: model filenames like `prediction_v1.json`

You can also test upload/download:

```bash
teelo model push models/prediction_v1.json
teelo model pull prediction_v1.json
```
