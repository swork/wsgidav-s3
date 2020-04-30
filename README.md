# WsgiDAV-S3

A wsgidav.DAVProvider implementation using Amazon Web Services (AWS) Simple
Storage Service (S3) as its backing store, suitable for deployment as an AWS
Lambda package behind AWS API Gateway, AWS Elastic Load Balancer or AWS
CloudFront.

## Deployment

### Specifying the provider

### AWS setup

AWSS3Provider and friends interact with AWS through the boto3 package. Deployed
outside of AWS infrastructure boto3 requires some account setup.
[https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html] gives
details (AWS CLI setup is overkill but helpful getting started).

Inside the AWS infrastructure (in an EC2 instance or a Lambda function package)
the boto3 package gets its configuration from the runtime environment.

### wsgidav configuration

Make reference in config.yaml (or similar) provider_mapping section, something
like this:

```yaml
provider_mapping:
    "/":
        provider: renlabs.wsgidav.AWSS3Provider
        kwargs: { bucket: dav.example.org,
                  root_prefix: davroot/,
                  readonly: false }
```

"bucket" is required.

"root_prefix" defaults to '/', and must end in a slash. AWSS3Provider actually
creates an object with the root_prefix value as its key, and arranges that all
other object keys begin with that value. The AWS S3 web console doesn't handle
"/" as an object key gracefully, so it's probably better to use a longer
root_prefix that does not begin with a slash.

