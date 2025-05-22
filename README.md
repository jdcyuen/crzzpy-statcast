# crzzpy-statcast

Git repository : 		E:\statcast\crzzpy-statcast
Remote Git repository:	https://github.com/jdcyuen/crzzpy-statcast
Google Cloud console:   https://console.cloud.google.com/welcome?project=crzzpy&pli=1&inv=1&invt=AbxYfQ






• Use the command pip install -r requirements.txt to install all packages listed in the requirements.txt file

1. Ensure gcloud CLI is Installed
	If you haven't installed the Google Cloud SDK, download and install it from:
	https://cloud.google.com/sdk/docs/install

2. Log in to Your Google Account
	Run the following command in your terminal or command prompt:

		gcloud auth login

    This will open a browser window where you can sign in with your Google account.

3. Set the Active Project (if needed)
		gcloud config set project [PROJECT_ID] Replace [PROJECT_ID] with your actual GCP project ID.

        gcloud config set project crzzpy
        

4. Verify Authentication
	Check the authenticated account:
		gcloud auth list
		
	Check the active project:
		gcloud config list project

5. Authenticate for Application Default Credentials (ADC)
    If you need authentication for APIs or services like BigQuery, Cloud Storage, or Compute Engine:

		gcloud auth application-default login
		
    This generates credentials for API access.


    Your browser has been opened to visit:

    https://accounts.google.com/o/oauth2/auth?response_type=code&client_id=764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com&redirect_uri=http%3A%2F%2Flocalhost%3A8085%2F&scope=openid+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fuserinfo.email+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fsqlservice.login&state=hsPyIQfTlP4utO1WAIZt85rmmnNFwU&access_type=offline&code_challenge=rw4OI_f_dluE1voD5edahjUJas-bakIrNCNeB-X6xog&code_challenge_method=S256


Credentials saved to file: [C:\Users\Joe\AppData\Roaming\gcloud\application_default_credentials.json]

```
{
  "account": "",
  "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
  "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
  "quota_project_id": "crzzpy",
  "refresh_token": "1//067hH5_JSGDEbCgYIARAAGAYSNwF-L9Ir1AtLX7P9tfVDag3a0mX9Y53UlpPjIW8Zhzuf-Hm_2xiBhVc2zpOXHgw3uBq7sPvzadE",
  "type": "authorized_user",
  "universe_domain": "googleapis.com"
}
```

These credentials will be used by any library that requests Application Default Credentials (ADC).

Quota project "crzzpy" was added to ADC which can be used by Google client libraries for billing and quota. Note that some services may still bill the project owning the resource.


-python statcast_fetch.py -h
-python statcast_fetch.py 2024-03-01 2024-03-30 --league milb --output statcast_data.csv
-python statcast_fetch.py 2024-03-01 2024-03-30 --league both


















