FROM amancevice/pandas:0.23.4-python3

RUN pip install synapseclient==2.0.0 \
                boto3==1.12.39 \
                git+https://github.com/larssono/bridgeclient.git@b90eb4c \
                git+https://github.com/Sage-Bionetworks/synapsebridgehelpers@5b0f487 \
                git+https://github.com/redcap-tools/PyCap.git@3d4964b
RUN git clone https://github.com/Sage-Bionetworks/at-home-pd.git /root/at-home-pd

CMD python /root/at-home-pd/user_add/user_add.py
