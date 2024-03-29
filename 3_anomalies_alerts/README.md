# Alerting system

The goal was to create a real-time alerting system that checks our data for anomalies live every 15 minutes and send reports to Telegram chat.

Metrics to check are: **DAU**, **views**, **likes**, **CTR**, **messages**.

To detect anomaly value I used the statistical **rule of sigmas** and **method of interquartile range**.

Run of the alert script is automated with **GitLab CI/CD**.

*anomaly_alert.ipynb* - script for alert reporting.

*[Rendered anomaly_alert.ipynb](https://nbviewer.org/github/EvgDubrovin/Data_Analyst_Simulator/blob/main/3_anomalies_alerts/anomaly_alert.ipynb)*

*anomaly_alert.png* - the view of anomaly reports in Telegram.

*.gitlab-ci.yml* - pipeline for running our script.
