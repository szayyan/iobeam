use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rand::RngCore;
use std::fs;
use std::io::Write;
use std::path::Path;

#[derive(Parser)]
#[command(name = "scripts")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate test data directories (small, med, large) populated with random files
    GenTestData {
        /// Path to an empty directory where test data will be generated
        path: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::GenTestData { path } => gen_test_data(&path)?,
    }

    Ok(())
}

fn gen_test_data(base_path: &str) -> Result<()> {
    let base = Path::new(base_path);
    let small_dir = base.join("small");
    let med_dir = base.join("med");
    let large_dir = base.join("large");

    for dir in [&small_dir, &med_dir, &large_dir] {
        fs::create_dir_all(dir)
            .with_context(|| format!("Failed to create directory {}", dir.display()))?;
    }

    let mut rng = rand::thread_rng();

    println!("Generating small files (100 files, <1 MB each)...");
    for i in 0..100 {
        let size = rng.next_u32() as usize % (1024 * 1024);
        write_random_file(&small_dir.join(format!("file_{i:03}.bin")), size, &mut rng)?;
    }

    println!("Generating medium files (10 files, 1–100 MB each)...");
    for i in 0..10 {
        let size = 1024 * 1024 + rng.next_u64() as usize % (99 * 1024 * 1024);
        write_random_file(&med_dir.join(format!("file_{i:02}.bin")), size, &mut rng)?;
    }

    println!("Generating large files (3 files, 1–2 GB each)...");
    for i in 0..3 {
        let size = 1024_usize * 1024 * 1024 + rng.next_u64() as usize % (1024 * 1024 * 1024);
        write_random_file(&large_dir.join(format!("file_{i}.bin")), size, &mut rng)?;
    }

    println!("Done.");
    Ok(())
}

fn write_random_file(path: &Path, size: usize, rng: &mut impl RngCore) -> Result<()> {
    let mut file = fs::File::create(path)
        .with_context(|| format!("Failed to create {}", path.display()))?;

    const CHUNK: usize = 8 * 1024 * 1024; // 8 MB write buffer
    let mut buf = vec![0u8; CHUNK];
    let mut remaining = size;

    while remaining > 0 {
        let to_write = remaining.min(CHUNK);
        rng.fill_bytes(&mut buf[..to_write]);
        file.write_all(&buf[..to_write])
            .with_context(|| format!("Failed to write to {}", path.display()))?;
        remaining -= to_write;
    }

    Ok(())
}
