# Description

The Redshift manifest tools allows to copy files that are unloaded to S3
 to local storage.  It uses a Redshift manifest file that is created by the unload
command to know which files to fetch.  


# Choices made
 - Self-made implementation of S3 client-side decryption by using pypi cryptography library.  There are Python
   libraries that help to do this S3 decryption however they work on S3 object level and will therefore load the whole
   object in memory.  The current implementation allows for a streaming decryption and therefore should be cheaper
   memory-wise and allows to use the boto3.s3.transfer which allows fast/parallel retrieving of files.  It also easily
   allows to implement the code for Python3
   
## Under consideration
 - Use boto3.s3.transfer for all file interactions, even when doing a `cat` operation on a file.  Initial
   implementation allowed to fetch file in memory if it was small enough as that would be faster however that is likely
   only a limited gain.

# Installation

This code has been written while using Python 3.6 therefore it is recommended to use Python 3 for this project.

```bash
git clone https://github.com/pvbouwel/redshift-manifest-tools.git
cd redshift-manifest-tools.git
sudo python3 ./setup.py install
```

If you receive an error when building `pycrypto` then install `gcc` on your host and try again.

# Example usage

This section will likely be extended in the future.

## Check usage of cli tool

An invalid usage should show help.  Specifying without parameters for example is an invalid invocation:

```bash
bash-3.2$ redshift-manifest-tools
NO ACTION SPECIFIED!
Defaulting to list available actions.  Please specify action using --action <action>
Available actions:
 - list-actions: Returns the list of supported actions
 - list-files: List the files mentioned in the manifest
 - retrieve-files: Retrieve files and store locally
	 * symmetric-key: Base 64 encoded symmetric key provided to unload data.  If provided to this tool then client side encryption is assumed
	 * dest: Target directory where to store files MANDATORY
	 * manifest-s3url: S3 path to manifest file MANDATORY
	 * overwrite: Flag to indicate whether local files should be overwritten
 - cat-files: Concatenate the files in manifest and print on stdout
	 * symmetric-key: Base 64 encoded symmetric key provided to unload data.  If provided to this tool then client side encryption is assumed
	 * manifest-s3url: S3 path to manifest file MANDATORY
```

## Retrieve [client-side encrypted](http://docs.aws.amazon.com/redshift/latest/dg/t_unloading_encrypted_files.html) files from a manifest and decrypt them

### First retrieve files without decrypting

```bash
bash-3.2$ redshift-manifest-tools --action retrieve-files --manifest-s3url 's3://manifest-tools/encrypted/encrypted.manifest' --dest /tmp/
bash-3.2$ cat /tmp/encrypted.000*
�Tu3
     ����!M(���7/���MY��s�r���1��� Ƚp@7eoz�9é@��~��k�('bash-3.2$
```

This shows that files are binary and not readable due to the encryption.

Retrieving the files again but this time decrypted by providing the symmetric key that was used in the unload command.  Since files are already present in `/tmp` use the `--overwrite` flag:

```bash
bash-3.2$ redshift-manifest-tools --action retrieve-files --manifest-s3url 's3://manifest-tools/encrypted/encrypted.manifest' --dest /tmp/ --symmetric-key 'cibeQ6J5GwJ8hLrrAdAbb09HjObumZGC/LuzM1RBKRA=' --overwrite
bash-3.2$ cat /tmp/encrypted.000* | sort
0|The
1|secret
2|is
3|42

```

# Releases
This will show the releases from newer to older

## 2.0.0
Given no big signs of usage just broke api-compatibility in file names and within the classes (refactoring method names).

 - Change from unittest to pytest
 - Improve overall structure have a EnvelopeFileCryptor that provides basic envelope decryption
 - Make S3EnvelopeFileCryptor a simple extension to allow the manifest functionality
 - Remove depency of pycrypto given it is not supported and has a bad reputation due to security issues (not that this 
 project was impacted by any of them)
 - Improve PEP-8 adherence
 - Switch to Google style docstrings.

## 1.0.0
 - Manage region in manifest apply region on manifest to annotate S3File objects
 - Increased major version number as breaking change has been done to the internal classes and this is the first version
 with the envisioned basic functionality.
 - Refactoring of the code to put all S3 interactions into the S3File class.
 - Adding support for decryption of encrypted files
 - Improve test coverage
 - Added support for python 3.4 (initial development was for 3.6 but python3.4 is more common and did not pass tests,
 Manifest was a bytes object instead of a string which gives problems with json.loads).

## 0.2.0
 - initial test version

# Issues/Contributions

Feedback is welcome.  If a bug is encountered just create an issue on the github repository.  If you can fix it yourself
you can create a pull request which I will review.  There are different tests that ship with the code, generally the
tests need to pass unless there is a good reason (e.g. test is flawed.  For example test `test_retrieve_manifest_euc1`
 at the moment does not pass except with owner account.  If you are interested in contributing check the development
 environment example to get easily started.
 
A lot of tests run against S3 for which access is currently not open. Passing unittests should be sufficient I can run
the remaining tests if a pull request is created.

## Development environment

This code is developed while using the Python 3.6 runtime .  It is likely most easy to get started with this version
or newer.  For easy setup instructions are provided for Amazon Linux.  There shouldn't be anything blocking
you from using another distribution (except possibly the additional effort to find the correct packages).

### OS packages

Install the Python development headers and gcc (probably optional was needed for pycrypto).
Install virtual environments to not pollute your OS completely

```bash
sudo yum -y install python36 python36-virtualenv.noarch gcc git
```

### Create a virtual environment

```bash
virtualenv-3.6 ~/venv36-redshift-manifest-tools
```

Everytime you want to develop using this virtual environment you need to activate it

```bash
. ~/venv36-redshift-manifest-tools/bin/activate
```

### Get the code

```bash
cd ~
git clone https://github.com/pvbouwel/redshift-manifest-tools.git
```

### Install the requirements and run the tests

```bash
cd ~/redshift-manifest-tools
pip install requirements -r requirements.txt
cd test
./run_tests.sh
```

Example output at the time of writing:

```bash
(venv37_redshit_manifest_tools)$ ./run_tests.sh 
VIRTUAL_ENV=/home/peter/venvs/venv37_redshift_manifest_tools
Running in VIRTUAL_ENV=/home/peter/venvs/venv37_redshift_manifest_tools
============================= test session starts ==============================
platform linux -- Python 3.7.1, pytest-4.6.2, py-1.8.0, pluggy-0.12.0
rootdir: /home/peter/code/git/github/redshift-manifest-tools
collected 20 items                                                             

test/test_decrypt_unittests.py .                                         [  5%]
test/test_manifest.py ...                                                [ 20%]
test/test_s3_file.py ..                                                  [ 30%]
test/test_s3_file_cryptor.py .                                           [ 35%]
test/test_s3_file_transfer.py .                                          [ 40%]
test/test_s3helper.py ............                                       [100%]

=============================== warnings summary ===============================
...
=================== 20 passed, 16 warnings in 6.43 seconds =====================
```

The warnings are deprecation warnings for depencies.