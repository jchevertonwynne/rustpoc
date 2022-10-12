use crate::grpc::voting_request::Vote;
use crate::grpc::voting_server::{Voting, VotingServer};
use futures_util::FutureExt;
use std::net::SocketAddr;
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinHandle;

use tonic::transport::{Error, Server};
use tonic::{Code, Request, Response, Status};

// reference: https://www.thorsten-hans.com/grpc-services-in-rust-with-tonic/

tonic::include_proto!("voting");

#[derive(Debug, Default, Clone)]
pub struct VotingService {}

#[tonic::async_trait]
impl Voting for VotingService {
    async fn vote(
        &self,
        request: Request<VotingRequest>,
    ) -> Result<Response<VotingResponse>, Status> {
        let request = request.into_inner();
        tracing::info!("received a vote request: {:?}", request);

        let vote = match Vote::from_i32(request.vote) {
            Some(vote) => vote,
            None => return Err(Status::new(Code::InvalidArgument, "invalid vote value")),
        };

        match vote {
            Vote::Up | Vote::Down => Ok(Response::new(VotingResponse {
                confirmation: format!("you voted {} for {}", vote.as_str_name(), request.url),
            })),
            Vote::Unknown => Err(Status::new(Code::InvalidArgument, "invalid vote value")),
        }
    }
}

pub fn run_server(address: SocketAddr, shutdown: Receiver<()>) -> JoinHandle<Result<(), Error>> {
    let server = Server::builder()
        .add_service(VotingServer::new(VotingService::default()))
        .serve_with_shutdown(address, shutdown.map(|inner| {
            match inner {
                Ok(_) => (),
                Err(err) => panic!("failed to receive shutdown signal: {:?}", err),
            }
        }));
    tokio::spawn(server)
}
