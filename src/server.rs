use tonic::{transport::Server, Request, Response, Status};
use sqlx::Row;

//nombre_paquete::nombre_servicio_server::{nombreservicio, nombreservicioServer}
use sqlcrud::sqlcrud_server::{Sqlcrud, SqlcrudServer};
use sqlcrud::{CreateRequest, CreateResponse};
use sqlcrud::{ReadRequest,   ReadResponse};
use sqlcrud::{UpdateRequest, UpdateResponse};
use sqlcrud::{DeleteRequest, DeleteResponse};

use std::fs::OpenOptions;
use std::io::{Write};

pub mod sqlcrud
{
    tonic::include_proto!("sqlcrud");
}

const URL : &str = "postgres://postgres:abc123@localhost:5432/gRPC_CRUD_TEST";
// const URL : &str = "postgres://public_user:1zamjSAP0BWJ@ep-icy-silence-09220265.us-east-2.aws.neon.tech/TestDB";
const PATH_OF_LOGFILE : &str = "./logfile.txt";
const MAX_CONNECTIONS: u32 = 1;

#[derive(Debug, Default)]
pub struct CRUDService
{
    pub pool: Option<sqlx::postgres::PgPool>,
}

//Implementacion de los metodos del servicio sqlcrud
#[tonic::async_trait]
impl Sqlcrud for CRUDService
{
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status>
    {
        println!("{}", self.pool.is_some());
        let mut message    : String = "OK".to_owned();
        let mut successful : bool   = true;
        println!("[i]Create request.\n{:?}", request);
        let req = request.into_inner();

        let response = match sqlx::query("INSERT INTO Users (username, email) VALUES ($1, $2) RETURNING id")
            .bind(req.username.clone().replace("\n", ""))
            .bind(req.email.clone().trim())
            .fetch_one(self.pool.as_ref().unwrap())
            .await
            {
                Ok(x) => x,
                Err(err) =>
                {
                    successful = false;
                    match err.as_database_error()
                    {
                        Some(err) => 
                        {
                            if err.is_unique_violation()
                            {
                                message = "The email is already registered.".to_owned();
                            }
                        },
                        None =>
                        {
                            message = "Unknow error.".to_owned();
                            println!("[!] Err to create user.\n{}", err);                        
                        }                        
                    };
                    let reply = CreateResponse {
                        successful,
                        id: 0,
                        message,
                    };
                    return Ok(Response::new(reply));
                }
            };
        let id : i32 = response.get("id");
        let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(PATH_OF_LOGFILE);
        if file.is_ok()
        {
            let mut file_unwraped = file.unwrap();
            if file_unwraped.write_all(format!("Created user[ id : {}, username: {}, email: {} ]\n", id, req.username.clone().replace("\n", "").replace("\r", ""), req.email.clone().trim()).as_bytes()).is_err()
            {
                println!("[!] Err on writeall.");
            }
        }
        let reply = CreateResponse {
            successful,
            id,
            message,
        };
        Ok(Response::new(reply))
    }
    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<ReadResponse>, Status>
    {
        println!("[i]Read request.\n{:?}", request);
        let mut message   : String = "OK".to_owned();
        let mut successful : bool = true;
        let req = request.into_inner();

        let response = match sqlx::query("
            SELECT id, username, email
            FROM users
            WHERE id = $1;
            ")
        .bind(req.id.clone())
        .fetch_one(self.pool.as_ref().unwrap())
        .await
        {
            Ok(x) => x,
            Err(err) =>
            {
                successful = false;
                match err.as_database_error()
                {
                    Some(err) => 
                    {
                        message = "Unknow error.".to_owned();
                        println!("[!] Unknow error\n{}", err);
                    },
                    None =>
                    {
                        message = format!("The id {} does not exist.", req.id).to_owned();
                    }                        
                };
                let reply = ReadResponse {
                    successful,
                    id: req.id,
                    username: "-".to_owned(),
                    email: "-".to_owned(),
                    message,
                };
                return Ok(Response::new(reply));
            }
        };
        let username : String = response.get("username");
        let email    : String = response.get("email");

        let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(PATH_OF_LOGFILE);
        if file.is_ok()
        {
            let mut file_unwraped = file.unwrap();
            if file_unwraped.write_all(format!("Read user[ id : {}, username: {}, email: {} ]\n", req.id, username.replace("\n", "").replace("\r", ""), email.trim()).as_bytes()).is_err()
            {
                println!("[!] Err on writeall.");
            }
            
        }

        let reply = ReadResponse {
            successful,
            id: req.id,
            username,
            email,
            message,
        };

        Ok(Response::new(reply))
    }
    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateResponse>, Status>
    {
        println!("[i]Update request.\n{:?}", request);
        let mut message    : String = "OK".to_owned();
        let mut successful : bool = true;
        let req = request.into_inner();

        _ = match sqlx::query("
            SELECT 1
            FROM users
            WHERE id = $1
            LIMIT 1;
            ")
        .bind(req.id.clone())
        .fetch_optional(self.pool.as_ref().unwrap())
        .await
        {
            Ok(x) => {
                if x.is_none()
                {
                    successful = false;
                    message    = "Don't exist that id on the DB.".to_owned();
                    let reply = UpdateResponse {
                        successful,
                        message,
                    };
                    return Ok(Response::new(reply));
                }
            },
            Err(_) =>
            {
                successful = false;
                message    = "Error in finding id on the DB.".to_owned();
                let reply = UpdateResponse {
                    successful,
                    message,
                };
                return Ok(Response::new(reply));
            }
        };

        match sqlx::query("
            UPDATE users
            SET username = $1, email = $2
            WHERE id = $3;
            ")
        .bind(req.username.clone().replace("\n", ""))
        .bind(req.email.clone().trim())
        .bind(req.id.clone())
        .execute(self.pool.as_ref().unwrap())
        .await
        {
            Ok(x) => x,
            Err(err) =>
            {
                successful = false;
                match err.as_database_error()
                {
                    Some(err) => 
                    {
                        message = "Unknow error.".to_owned();
                        println!("[!] Unknow error\n{}", err);
                    },
                    None =>
                    {
                        message = "The id does not exist.".to_owned();
                    }                        
                };
                let reply = UpdateResponse {
                    successful,
                    message,
                };
                return Ok(Response::new(reply));
            }
        };

        let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(PATH_OF_LOGFILE);
        if file.is_ok()
        {
            let mut file_unwraped = file.unwrap();
            if file_unwraped.write_all(format!("Update user[ id : {}, username: {}, email: {} ]\n", req.id, req.username.replace("\n", "").replace("\r", ""), req.email.trim()).as_bytes()).is_err()
            {
                println!("[!] Err on writeall.");
            }
            if file_unwraped.flush().is_err()
            {
                println!("[!] Err on flush file.");
            }
        }
        
        let reply = UpdateResponse {
            successful,
            message,
        };

        Ok(Response::new(reply))
    }
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status>
    {
        println!("[i]Delete request.\n{:?}", request);
        let mut message    : String = "OK".to_owned();
        let mut successful : bool = true;
        let req = request.into_inner();

        _ = match sqlx::query("
            SELECT 1
            FROM users
            WHERE id = $1
            LIMIT 1;
            ")
        .bind(req.id.clone())
        .fetch_optional(self.pool.as_ref().unwrap())
        .await
        {
            Ok(x) => {
                if x.is_none()
                {
                    successful = false;
                    message    = "Don't exist that id on the DB.".to_owned();
                    let reply = DeleteResponse {
                        successful,
                        message,
                    };
                    return Ok(Response::new(reply));
                }
            },
            Err(_) =>
            {
                successful = false;
                message    = "Error in finding id on the DB.".to_owned();
                let reply = DeleteResponse {
                    successful,
                    message,
                };
                return Ok(Response::new(reply));
            }
        };

        match sqlx::query("
            DELETE FROM users
            WHERE id = $1;
            ")
        .bind(req.id.clone())
        .execute(self.pool.as_ref().unwrap())
        .await
        {
            Ok(x) => x,
            Err(err) =>
            {
                successful = false;
                match err.as_database_error()
                {
                    Some(err) => 
                    {
                        message = "Unknow error.".to_owned();
                        println!("[!] Unknow error\n{}", err);
                    },
                    None =>
                    {
                        message = "The id does not exist.".to_owned();
                    }                        
                };
                let reply = DeleteResponse {
                    successful,
                    message,
                };
                return Ok(Response::new(reply));
            }
        };

        let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(PATH_OF_LOGFILE);
        if file.is_ok()
        {
            let mut file_unwraped = file.unwrap();
            if file_unwraped.write_all(format!("Delete user[ id : {} ]\n", req.id).as_bytes()).is_err()
            {
                println!("[!] Err on writeall.");
            }
            if file_unwraped.flush().is_err()
            {
                println!("[!] Err on flush file.");
            }
        }

        let reply = DeleteResponse {
            successful,
            message,
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>  
{
    let addr = "[::1]:50051".parse()?;
    println!("Listening on {}...", addr);
    let pool = match sqlx::postgres::PgPoolOptions::new()
               .max_connections(MAX_CONNECTIONS)
               .connect(URL)
               .await
               {
                   Ok(x) => x,
                   Err(err) => 
                   {
                       panic!("[!] Error to connect to the postgres database.\n{}", err);
                   },
               };
    // let pool = match sqlx::postgres::PgPool::connect(URL)
    //         .await
    //         {
    //             Ok(x) => x,
    //             Err(err) => 
    //             {
    //                 panic!("[!] Error to connect to the postgres database.\n{}", err);
    //             },
    //         };
    let testfield_service = CRUDService{pool: Some(pool)};
    
    Server::builder()
        .add_service(SqlcrudServer::new(testfield_service))
        .serve(addr)
        .await?;

    Ok(())
}