from distutils.core import setup
from setuptools import find_packages

setup(
    name='parser',  # How you named your package folder (MyLib)
    packages=find_packages(),  # Chose the same as "name"
    version='0.2',  # Start with a small number and increase it with every change you make
    license='MIT',  # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    description='Parses over control files to create dynamic DAGs.',  # Give a short description about your library
    author='Dillon Johnson',  # Type in your name
    author_email='dillonjohnson1015@gmail.com',  # Type in your E-Mail
    url='https://github.com/dillonjohnson/parser',  # Provide either the link to your github or to your website
    download_url='https://github.com/user/reponame/archive/v_01.tar.gz',  # todo   # I explain this later on
    keywords=['apache-airflow', 'parser'],  # Keywords that define your package best
    install_requires=[  # I get to this in a second
        'apache-airflow',
        'xmltodict'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Intended Audience :: Developers',  # Define that your audience are developers
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',  # Again, pick a license
        'Programming Language :: Python :: 3',  # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
