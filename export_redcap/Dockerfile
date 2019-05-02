FROM philsnyder/at-home-pd:latest
RUN pip install synapseclient git+https://github.com/redcap-tools/PyCap.git

CMD python /root/at-home-pd/export_redcap/export_redcap.py
