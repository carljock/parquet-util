use clap::{Parser, Subcommand};
mod commands;

#[derive(Parser)]
#[command(name = "pqcli", about = "Apache Parquet CLI utility")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// View the contents of a Parquet file
    View(commands::view::ViewArgs),
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::View(args) => commands::view::run(args),
    }
}
