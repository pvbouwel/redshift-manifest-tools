# Description

The Redshift manifest tools allows to copy files that are unloaded to S3
 to local storage.  It uses a Redshift manifest file that is created by the unload
command to know which files to fetch.  


# Choices made
 - Self-made implementation of S3 client-side decryption by using default python crypto libraries.  There are Python
   libraries that help to do this S3 decryption however they work on S3 object level and will therefore load the whole
   object in memory.  The current implementation allows for a streaming decryption and therefore should be cheaper
   memory-wise and allows to use the boto3.s3.transfer which allows fast/parallel retrieving of files.  It also easily
   allows to implement the code for Python3
   
## Under consideration
 - Use boto3.s3.transfer for all file interactions, even when doing a `cat` operation on a file.  Initial
   implementation allowed to fetch file in memory if it was small enough as that would be faster however that is likely only a limited gain.

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
EXv����z��̇�y.��sy44�y�O˩����|�B�2{C{7�0̣-C�G�J�%�z'bash-3.2$
```

This shows that files are binary and not readable due to the encryption.

Retrieving the files again but this time decrypted by providing the symmetric key that was used in the unload command.  Since files are already present in `/tmp` use the `--overwrite` flag:

```bash
bash-3.2$ redshift-manifest-tools --action retrieve-files --manifest-s3url 's3://manifest-tools/encrypted/encrypted.manifest' --dest /tmp/ --symmetric-key 'cibeQ6J5GwJ8hLrrAdAbb09HjObumZGC/LuzM1RBKRA=' --overwrite
bash-3.2$ cat /tmp/encrypted.000*
1
2
3
4
```

# Releases
This will show the releases from newer to older

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
 environment example to get easily started

## Development environment

This code is developed while using the Python 3.4.3 runtime .  It is likely most easy to get started with this version
or newer.  For easy setup instructions are provided for Amazon Linux.  There shouldn't be anything blocking
you from using another distribution (except possibly the additional effort to find the correct packages).

### OS packages

Install the Python development headers and gcc to allow compilation of pycrypto libraries.  Also install virtual
environments to not pollute your OS completely

```bash
sudo yum -y install python34 python34-virtualenv.noarch gcc git
```

### Create a virtual environment

```bash
virtualenv-3.4 ~/venv34-redshift-manifest-tools
```

Everytime you want to develop using this virtual environment you need to activate it

```bash
. ~/venv34-redshift-manifest-tools/bin/activate
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
(venv34-redshift-manifest-tools)[ec2-user@ip-172-31-9-77 test]$ ./run_tests.sh
VIRTUAL_ENV=/home/ec2-user/venv34-redshift-manifest-tools
Running in VIRTUAL_ENV=/home/ec2-user/venv34-redshift-manifest-tools
You are using pip version 6.0.8, however version 9.0.1 is available.
You should consider upgrading via the 'pip install --upgrade pip' command.
You are using pip version 6.0.8, however version 9.0.1 is available.
You should consider upgrading via the 'pip install --upgrade pip' command.
Collecting nose
  Downloading nose-1.3.7-py3-none-any.whl (154kB)
    100% |################################| 155kB 2.3MB/s
Installing collected packages: nose

Successfully installed nose-1.3.7
..............E....
======================================================================
ERROR: test_retrieve_manifest_euc1 (S3Helper_tests.S3HelperTestCase)
----------------------------------------------------------------------
```

Followed by printout of exception and logging
