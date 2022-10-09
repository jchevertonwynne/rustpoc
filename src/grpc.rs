use std::net::SocketAddr;
use crate::grpc::voting_request::Vote;
use crate::grpc::voting_server::{Voting, VotingServer};

use tonic::{Code, Request, Response, Status};
use tonic::transport::Server;

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
        tracing::info!("received a vote reqeust: {:?}", request);

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

pub fn run_server(address: SocketAddr) {
    let server = Server::builder()
        .add_service(VotingServer::new(VotingService::default()))
        .serve(address);
    tokio::spawn(server);
}