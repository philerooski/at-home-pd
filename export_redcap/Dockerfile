FROM amancevice/pandas:0.23.4-python3

RUN pip install synapseclient git+https://github.com/redcap-tools/PyCap.git
RUN git clone https://github.com/Sage-Bionetworks/at-home-pd.git /root/at-home-pd

CMD python /root/at-home-pd/export_redcap/export_redcap.py
