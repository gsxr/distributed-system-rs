use std::{env, error::Error};

use dotenv::dotenv;
use futures::future;
use redis::{AsyncTypedCommands, Client, streams::StreamReadOptions};
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() {
    dotenv().ok();
    let redis_url = env::var("REDIS_URL").expect("REDIS_URL is required");
    let client = redis::Client::open(redis_url).expect("Failed to connect to redis server");
    let mut conn = client
        .get_multiplexed_tokio_connection()
        .await
        .expect("Failed to get multiplexed connection");
    let _ = conn.flushall().await;

    let stream_name = "test-stream".to_string();
    let group_name = "main_group".to_string();
    let max_consumers = 10;
    let max_msgs = 20;

    let _ = create_group(&client, stream_name.clone(), group_name.clone()).await;

    let mut join_handles = vec![];
    for i in 0..max_consumers {
        join_handles.push(
            run_consumer(
                &client,
                stream_name.clone(),
                group_name.clone(),
                format!("consumer_{}", i + 1 / 10),
            )
            .await
            .unwrap(),
        );
    }

    {
        let a = tokio::task::spawn(async move {
            let conn = client.get_multiplexed_async_connection().await;
            if let Ok(mut conn) = conn {
                for i in 0..max_msgs {
                    let _ = conn
                        .xadd(stream_name.clone(), "*", &[("SERVER_REQUEST", i)])
                        .await;
                }
            }
        });
        join_handles.push(a);
    }

    future::join_all(join_handles).await;
}

pub async fn create_group(
    client: &Client,
    stream_name: String,
    group_name: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    let _conn_group = conn
        .xgroup_create_mkstream(stream_name, group_name, "0")
        .await;
    Ok(())
}

pub async fn run_consumer(
    client: &Client,
    stream_name: String,
    group_name: String,
    consumer_name: String,
) -> Result<JoinHandle<()>, Box<dyn Error + Send + Sync>> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    let options = StreamReadOptions::default()
        .block(0)
        .group(group_name.clone(), consumer_name.clone());
    let handle = tokio::spawn(async move {
        loop {
            let res = conn
                .xread_options(&[stream_name.clone()], &[">"], &options)
                .await;
            if let Ok(b) = res {
                if let Some(b) = b {
                    for stream_key in b.keys {
                        for stream_id in stream_key.ids {
                            let _del_entry = conn
                                .xdel(stream_name.clone(), &[stream_id.id.clone()])
                                .await;
                            let _ack_res = conn
                                .xack(
                                    stream_name.clone(),
                                    group_name.clone(),
                                    &[stream_id.id.clone()],
                                )
                                .await;
                            println!(
                                "{stream_name} {group_name} {consumer_name} {:#?} {:#?}",
                                stream_id.id.clone(),
                                stream_id.map
                            );
                        }
                    }
                }
            }
        }
    });
    return Ok(handle);
}
