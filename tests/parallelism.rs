use core::panic;

use sqlcrud::sqlcrud_client::SqlcrudClient;
use sqlcrud::CreateRequest;
use sqlcrud::ReadRequest;
use sqlcrud::UpdateRequest;
use sqlcrud::DeleteRequest;

pub mod sqlcrud
{
    tonic::include_proto!("sqlcrud");
}
const NUM_OF_USERS : u32 = 5000;
const ADDR : &str = "http://[::1]:50051";
async fn parallelism_test_create() -> i32
{
    let mut handles = vec![];
    let min_index = std::sync::Arc::new(std::sync::Mutex::new(i32::max_value()));
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(NUM_OF_USERS as usize)); // Limit concurrency to 4 tasks
    let channel = tonic::transport::Channel::from_static(ADDR)
        .connect()
        .await.expect("[!] Error on create client channel.\n");
    for i in 0..NUM_OF_USERS
    {
        let min_index_clone = std::sync::Arc::clone(&min_index);
        let semaphore_clone = std::sync::Arc::clone(&semaphore);
        let channel_clone   = channel.clone();
        let handle = tokio::spawn(async move{
            let _permit = semaphore_clone.acquire().await.unwrap();
            let mut client = SqlcrudClient::new(channel_clone.clone());
            //let mut client = SqlcrudClient::connect("http://[::1]:50051").await.expect("[!] Error on create client.\n");
            let request = tonic::Request::new(
                CreateRequest {
                    username: i.clone().to_string(),
                    email   : i.clone().to_string(),
                }
            );
            let response = client.create(request).await.expect("[!] Err to make create request");
            let create_response = response.into_inner().clone();
            if create_response.clone().id < *min_index_clone.lock().unwrap()
            {
                *min_index_clone.lock().unwrap() = create_response.clone().id;
            }
            if !create_response.clone().successful
            {
                panic!("[!] Err to create user.\n{}", create_response.clone().message);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
    let out : i32 = *min_index.lock().unwrap();
    out
}
async fn parallelism_test_read(min_index : i32)
{
    let mut handles = vec![];
    let channel = tonic::transport::Channel::from_static(ADDR)
        .connect()
        .await.expect("[!] Error on read client channel.\n");
    for i in 0..NUM_OF_USERS
    {
        let channel_clone   = channel.clone();
        let handle = tokio::spawn(async move{
            let mut client = SqlcrudClient::new(channel_clone.clone());
            //let mut client = SqlcrudClient::connect("http://[::1]:50051").await.expect("[!] Error on read client.\n");
            let id = (min_index as i32) + (i.clone() as i32);
            let request = tonic::Request::new(
                ReadRequest {
                    id: id.clone()
                }
            );
            let response = client.read(request).await.expect("[!] Err to make read request");
            let resp = response.into_inner();
            if !resp.clone().successful
            {
                panic!("[!] Err to read user.\nintern fields:\n\t min_index: {}\nfields:\n\tid: {}\n {}", min_index.clone(), id.clone(), resp.clone().message);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
async fn parallelism_test_update(min_index : i32)
{
    let mut handles = vec![];
    let channel = tonic::transport::Channel::from_static(ADDR)
        .connect()
        .await.expect("[!] Error on update client channel.\n");
    for i in 0..NUM_OF_USERS
    {
        let channel_clone   = channel.clone();
        let handle = tokio::spawn(async move{
            let mut client = SqlcrudClient::new(channel_clone.clone());
            //let mut client = SqlcrudClient::connect("http://[::1]:50051").await.expect("[!] Error on update client.\n");
            let request = tonic::Request::new(
                UpdateRequest {
                    id: ((min_index as i32) + (i.clone() as i32)),
                    username:(min_index + (i as i32)).to_string(),
                    email: (min_index + (i as i32)).to_string(),
                }
            );
            let response = client.update(request).await.expect("[!] Err to make update request");
            let resp = response.into_inner();
            if !resp.clone().successful
            {
                panic!("[!] Err to update user.\n {}", resp.clone().message);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
async fn parallelism_test_delete(min_index : i32)
{
    let mut handles = vec![];

    let channel = tonic::transport::Channel::from_static(ADDR)
        .connect()
        .await.expect("[!] Error on delete client channel.\n");
    for i in 0..NUM_OF_USERS
    {
        let channel_clone   = channel.clone();
        let handle = tokio::spawn(async move{
            let mut client = SqlcrudClient::new(channel_clone.clone());
            //let mut client = SqlcrudClient::connect("http://[::1]:50051").await.expect("[!] Error on delete client.\n");
            let request = tonic::Request::new(
                DeleteRequest {
                    id: (min_index + (i.clone() as i32)),
                }
            );
            let response = client.delete(request).await.expect("[!] Err to make delete request");
            let resp = response.into_inner();
            if !resp.clone().successful
            {
                panic!("[!] Err to delete user.\n {}", resp.clone().message);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
#[tokio::test]
async fn parallelism_test()
{
    println!("Creating {} users...", NUM_OF_USERS);
    let min_id : i32 = parallelism_test_create().await;
    println!("{} users created.", NUM_OF_USERS);
    println!("Reading {} users...", NUM_OF_USERS);
    parallelism_test_read(min_id).await;
    println!("{} users readed.", NUM_OF_USERS);
    println!("Updating {} users...", NUM_OF_USERS);
    parallelism_test_update(min_id).await;
    println!("{} users updated.", NUM_OF_USERS);
    println!("Deleting {} users...", NUM_OF_USERS);
    parallelism_test_delete(min_id).await;
    println!("{} users deleted.", NUM_OF_USERS);
    assert!(true);
}