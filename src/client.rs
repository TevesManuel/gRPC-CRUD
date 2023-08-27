use sqlcrud::sqlcrud_client::SqlcrudClient;
use sqlcrud::CreateRequest;
use sqlcrud::ReadRequest;
use sqlcrud::UpdateRequest;
use sqlcrud::DeleteRequest;
pub mod sqlcrud
{
    tonic::include_proto!("sqlcrud");
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>
{ 
    let mut client = SqlcrudClient::connect("http://[::1]:50051").await?;
    loop
    {
        println!("Types of requests:\n\t[0]-Create\n\t[1]-Read\n\t[2]-Update\n\t[3]-Delete\nType of request to make:");
        let mut op : String = String::new();
        std::io::stdin().read_line(&mut op).expect("[!] Error on read input.");
        if op.contains("0")
        {
            println!("Fields:");
            println!("\tusername:");
            let mut username : String = String::new();
            std::io::stdin().read_line(&mut username).expect("[!] Error on read input.");
            println!("\temail:");
            let mut email : String = String::new();
            std::io::stdin().read_line(&mut email).expect("[!] Error on read input.");
            let request = tonic::Request::new(
                CreateRequest {
                    username,
                    email,
                }
            );
            let response = client.create(request).await?; 
            println!("----------------------------------------------------------------------");
            println!("RESPONSE");
            println!("----------------------------------------------------------------------\n{:?}", response);
            println!("----------------------------------------------------------------------");
        }
        else if op.contains("1")
        {
            println!("Fields:");
            println!("\tid:");
            let mut id_s : String = String::new();
            std::io::stdin().read_line(&mut id_s).expect("[!] Error on read input.");
            id_s = id_s.trim().to_string();
            let id : i32 = id_s.parse::<i32>().expect("Error to convert input to integer");
            let request = tonic::Request::new(
                ReadRequest {
                    id,
                }
            );
            let response = client.read(request).await?; 
            println!("----------------------------------------------------------------------");
            println!("RESPONSE");
            println!("----------------------------------------------------------------------\n{:?}", response);
            println!("----------------------------------------------------------------------");
        }
        else if op.contains("2")
        {
            println!("Fields:");
            println!("\tid:");
            let mut id_s : String = String::new();
            std::io::stdin().read_line(&mut id_s).expect("[!] Error on read input.");
            id_s = id_s.trim().to_string();
            let id : i32 = id_s.parse::<i32>().expect("Error to convert input to integer");
            println!("\tusername:");
            let mut username : String = String::new();
            std::io::stdin().read_line(&mut username).expect("[!] Error on read input.");
            println!("\temail:");
            let mut email : String = String::new();
            std::io::stdin().read_line(&mut email).expect("[!] Error on read input.");
            let request = tonic::Request::new(
                UpdateRequest {
                    id,
                    username,
                    email,
                }
            );
            let response = client.update(request).await?; 
            println!("----------------------------------------------------------------------");
            println!("RESPONSE");
            println!("----------------------------------------------------------------------\n{:?}", response);
            println!("----------------------------------------------------------------------");
        }
        else if op.contains("3")
        {
            println!("Fields:");
            println!("\tid:");
            let mut id_s : String = String::new();
            std::io::stdin().read_line(&mut id_s).expect("[!] Error on read input.");
            id_s = id_s.trim().to_string();
            let id : i32 = id_s.parse::<i32>().expect("Error to convert input to integer");
            let request = tonic::Request::new(
                DeleteRequest {
                    id,
                }
            );
            let response = client.delete(request).await?; 
            println!("----------------------------------------------------------------------");
            println!("RESPONSE");
            println!("----------------------------------------------------------------------\n{:?}", response);
            println!("----------------------------------------------------------------------");
        }
    }
}