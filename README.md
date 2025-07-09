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

To set up a virtual environment:
1. python -m venv .venv
2. .\.venv\Scripts\activate
3. install required libraries in virtual environment

* pip freeze > requirements.txt
* pip install -r requirements.txt

https://www.activestate.com/resources/quick-reads/how-to-uninstall-python-packages/
To uninstall a package: 

* pip uninstall <packagename>

* to stop working in virtual environment type: deactivate
* to remove virtual environment type: rm -rf .venv

✅ 1. Ensure gcloud is installed and authenticated
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud config list

✅ 2. Test locally with command-line
Make sure your function runs correctly from the command line:
python -m src.statcast_fetch 2024-03-01 2024-03-30 --league both

✅ 3. Freeze dependencies into requirements.txt
If not already done:
pip freeze > requirements.txt

✅ 4. Check file structure
Ensure you have the following:
main.py                 ← GCF entry point
src/statcast_fetch.py   ← Contains core logic
requirements.txt

And your main.py should expose an HTTP function like:
def run_statcast(request): ...

✅ 5. (Optional) Add .gcloudignore
Exclude unnecessary files:
__pycache__/
*.pyc
.env
tests/
*.md

✅ 6. Deploy
Run this from the root directory (crzzpy-statcast/):

gcloud functions deploy run_statcast \
  --runtime python311 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point run_statcast \
  --region=us-central1 \
  --source=.


To trigger the GC cloud function:

curl -X POST YOUR_CLOUD_FUNCTION_URL \
  -H "Content-Type: application/json" \
  -d '{"start_date":"2024-04-01", "end_date":"2024-04-05", "league":"mlb", "file":"statcast_mlb.csv"}'


To get POST YOUR_CLOUD_FUNCTION_URL:

 Option 1: Immediately After Deployment

  After running:

    gcloud functions deploy run_statcast \
      --runtime python311 \
      --trigger-http \
      --allow-unauthenticated \
      --entry-point run_statcast \
      --region=us-central1 \
      --source=.
  This will deploy your GC cloud function

  The CLI output will include something like this:

  Deploying function (may take a while)...done.
  availableMemoryMb: 256
  entryPoint: run_statcast
  httpsTrigger:
    url: https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/run_statcast
  ...


  Copy that URL — that’s your function’s HTTP endpoint.

Option 2: Use gcloud functions describe
  Run this to get the URL at any time:
    gcloud functions describe run_statcast --region=us-central1 --format="value(httpsTrigger.url)"

  This will return just the URL, like:

  https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/run_statcast


Option 3: Google Cloud Console
  Go to Google Cloud Console → Cloud Functions.

  Click on your function (run_statcast).

  The URL is shown under the Trigger section.

  Once you have it, you can send requests to it like this:

    curl -X POST YOUR_URL -H "Content-Type: application/json" -d '{"start_date":"2024-04-01","end_date":"2024-04-



Download and Install Pip on macOS:

* python3 -m ensurepip --upgrade

New changes:

* Requests are chunked and made in parallel
* New progress bars

7/9/2025
* Add clean_dataframe method, replace nan in data with none


Steps to run on MacOS:

1. download zip file from https://github.com/jdcyuen/crzzpy-statcast, unzip to a different folder, open the folder
2. pip3 --version to make sure you have pip installed, skip if you are sure you have it
3. pip3 install -r requirements.txt,  this will install any new packages that has been added to the new version of the python script.
4. python3 -m src.statcast_fetch 2024-03-01 2024-03-30 --league both

















