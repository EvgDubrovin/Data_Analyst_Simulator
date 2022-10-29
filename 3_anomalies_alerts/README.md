# Alerting system

We need to create a real-time alerting system, that checks our data for anomalies live every 15 minutes and send reports to Telegram chat.

Metrics to check are: **DAU**, **views**, **likes**, **CTR**, **messages**.

To detect anomaly value I used the statistical **rule of sigmas** and **method of interquartile range**.

The alert script running is automated with **GitLab CI/CD**.

*anomalies_alert.ipynb* - script for alert reporting.

*[Rendered anomalies_alert.ipynb](https://nbviewer.org/github/EvgDubrovin/Data_Analyst_Simulator/blob/main/3_anomalies_alerts/anomalies_alert.ipynb)*

*anomaly_alert.png* - the view of anomaly reports in Telegram.

*.gitlab-ci.yml* - pipeline for running our script.
