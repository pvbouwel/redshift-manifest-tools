from setuptools import setup, find_packages
"""
Thanks to the nice write up of https://kushaldas.in/posts/building-command-line-tools-in-python-with-click.html
"""

setup(
    name="redshift-manifest-tools",
    version='2.0.0',
    py_modules=['redshift_manifest_tools'],
    install_requires=[
        'Click',
        'boto3',
        'cryptography'
    ],
    packages=find_packages(),
    entry_points='''
        [console_scripts]
        redshift-manifest-tools=redshift_manifest_tools:cli_main
    ''',
)
