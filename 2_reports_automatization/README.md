# Reports automatization

Created automatic reports sending with **Telegram-bot** and **GitLab CI/CD**. 

## 1) A news feed report:
* text message with key metrics (DAU, views, likes, CTR)
* charts with metrics values for the last 7 days

*feed_report.ipynb* - feed reporting script

*[Rendered feed_report.ipynb](https://nbviewer.org/github/EvgDubrovin/Data_Analyst_Simulator/blob/main/2_reports_automatization/feed_report.ipynb)*

*feed_report.png* - view of everyday reports in Telegram

## 2) The whole app report:
* text messages and charts with key metrics for yesterday
* text messages and charts with key metrics for a time period 

*common_report.ipynb* - common reporting script

*[Rendered common_report.ipynb](https://nbviewer.org/github/EvgDubrovin/Data_Analyst_Simulator/blob/main/2_reports_automatization/common_report.ipynb)*

*common_report.png* - view of everyday reports for the whole app in Telegram



*.gitlab-ci.yml* - automatization script for GitLab CI/CD.
