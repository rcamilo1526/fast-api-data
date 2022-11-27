# DATA API REST

To run this project, you'll need to have [Docker](https://docs.docker.com/get-docker/) installed.

## Getting started

Build the container, providing a tag:  
`docker build -t data_api:0.1 .`

Then you can run the container, passing in a name for the container, and the previously used tag:  
`docker run -p 8000:8000 --name my-api data_api:0.1`

Note that if you used the code as-is with the `--reload` option that you won't be able to kill the container using `CTRL + C`.  
Instead in another terminal window you can kill the container using Docker's kill command:  
`docker kill my-api`




![](https://img.shields.io/badge/Language-Python-informational?style=flat&logo=python&logoColor=white&color=2496ED)
![](https://img.shields.io/badge/Framework-FastAPI-informational?style=flat&logo=fastapi&logoColor=white&color=2496ED)
![](https://img.shields.io/badge/Tools-Docker-informational?style=flat&logo=docker&logoColor=white&color=2496ED)
### About me
 

You can find me on [![LinkedIn][1.1]][1] or on [![Kaggle][2.1]][2].

My gitlab :see_no_evil: [![LinkedIn][3.1]][3] 

<!-- Icons -->

[1.1]: https://img.shields.io/badge/LinkedIn-0077B5?style=plastic&logo=linkedin&logoColor=white

[1]: https://www.linkedin.com/in/raul-camilo-martin-bernal/

[2.1]: https://img.shields.io/badge/Kaggle-20BEFF?style=plastic&logo=kaggle&logoColor=white

[2]: https://www.kaggle.com/rmartin1526

[3.1]: https://img.shields.io/badge/GitLab-FCA121?style=plastic&logo=gitlab&logoColor=white

[3]: https://gitlab.com/rcamilo1526
