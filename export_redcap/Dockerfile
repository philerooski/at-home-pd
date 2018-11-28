FROM amancevice/pandas:0.23.4-python3

RUN pip install synapseclient git+https://github.com/redcap-tools/PyCap.git
RUN git clone https://github.com/philerooski/at-home-pd.git /root/at-home-pd

CMD python /root/at-home-pd/tag_users/tag_users.py
