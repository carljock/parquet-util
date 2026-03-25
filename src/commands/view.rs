use std::fs::File;
use clap::Args;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::util::pretty::print_batches;

#[derive(Args)]
pub struct ViewArgs {
    /// Path to the Parquet file
    file: std::path::PathBuf,

    /// Max rows to display (default: 1000, use 0 for all)
    #[arg(short = 'n', long, default_value_t = 1000)]
    limit: usize,
}

pub fn run(args: ViewArgs) -> anyhow::Result<()> {
    let file = File::open(&args.file)
        .map_err(|e| anyhow::anyhow!("Cannot open '{}': {}", args.file.display(), e))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Print schema
    println!("Schema:");
    for field in builder.schema().fields() {
        println!("  {}: {}", field.name(), field.data_type());
    }
    println!();

    let row_limit = if args.limit == 0 { usize::MAX } else { args.limit };
    let reader = builder.build()?;

    let mut batches = Vec::new();
    let mut total_rows: usize = 0;
    let mut truncated = false;

    for batch_result in reader {
        let batch = batch_result?;
        let remaining = row_limit.saturating_sub(total_rows);
        if remaining == 0 { truncated = true; break; }
        if batch.num_rows() > remaining {
            batches.push(batch.slice(0, remaining));
            total_rows += remaining;
            truncated = true;
            break;
        }
        total_rows += batch.num_rows();
        batches.push(batch);
    }

    print_batches(&batches)?;

    if truncated {
        println!("\n[Showing first {} rows. Use --limit 0 for all rows.]", total_rows);
    } else {
        println!("\n[{} row(s) total]", total_rows);
    }
    Ok(())
}
