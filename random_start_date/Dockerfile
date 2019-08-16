FROM rocker/tidyverse:3.6.1

RUN apt install libffi-dev
RUN Rscript -e 'install.packages("synapser", repos=c("http://ran.synapse.org", "http://cran.fhcrc.org"))'
RUN git clone https://github.com/Sage-Bionetworks/at-home-pd.git /root/at-home-pd

CMD Rscript /root/at-home-pd/random_start_date/random_start_date.R
