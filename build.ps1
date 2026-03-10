Remove-Item -Recurse -Force build, dist -ErrorAction Ignore
Remove-Item jira-support-dashboard.spec -ErrorAction Ignore

pyinstaller --onefile --name jira-support-dashboard `
--add-data "templates;templates" `
--add-data "static;static" `
--hidden-import src.api `
--hidden-import src.config `
--hidden-import src.db `
--hidden-import src.jira_client `
--hidden-import src.sync `
launcher.py

Copy-Item .\.env.example .\dist\.env