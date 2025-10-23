use fyntr::run;

#[actix_rt::main]
async fn main() -> Result<(), anyhow::Error> {
    run::server().await
}
