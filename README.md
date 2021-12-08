# gsutil-concat
Automation script to concatenate files in GCS and save new file in same directory

#Pre-requisites:
- Ensure you have Google Cloud SDK installed. You can check if you have it installed by typing 'gcloud' (no quotes) in your terminal. If a bunch of information comes up, including 'Available commands for gcloud' then you are set! If not, you can find installation details here: https://cloud.google.com/sdk/docs/install. Check that this installed correctly by typing 'gcloud' in your terminal again. If you receive the error, 'command not found', please refer to the 'troubleshooting' section in the Confluence documentation.
- Ensure you have 2.7.15 or higher installed on your machine. If you think you already have Python installed, you can check the version by typing 'python --version' (no quotes) in your terminal. If you do not, simply search for "Python install Mac" and download/install the latest version.
- There are a few python packages you will need to install to be able to use them in the script. It's easy to install a Python package. Simply run "pip install modulename" in your terminal. Use 'pip' if you have Python version 2 or lower and use 'pip3' if you have Python version 3 or higher. The below modules need to be installed by running these commands:
	- pip install google.cloud
	- pip install google-cloud-bigquery
	- pip install numpy
	- pip install pandas
	- pip install datetime
	- pip install gcsfs
- You will need to authenticate your Google account to be able to access our data in Google Cloud. This is simple - just type 'gcloud auth application-default login' (no quotes) into your terminal. A new page will appear in your browser and ask you to select your Google account that you'd liek to authenticate. Click your  email account. You will then be brought to a new page that says you have been authenticated!
