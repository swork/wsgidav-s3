from setuptools import setup, find_packages

setup(
    name="wsgidav-s3",
    version="0.0.2",
    packages=['renlabs.wsgidav'],
    author="Steve Work",
    author_email="steve@work.renlabs.com",
    description="wsgidav.DAVProvider for Amazon Web Services S3 bucket content",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    install_requires=['wsgidav', 'boto3'],
    zip_safe=True,
)
