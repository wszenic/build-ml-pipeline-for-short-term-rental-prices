#!/usr/bin/env python
"""
Perform basic cleaning and save results to W&B

Date: 11.11.2022
Author: Wojciech Szenic
"""
import argparse
import logging
import tempfile

import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    """
    Perform basic cleaning and save results to W&B
    """
    logger.info(f"Downloading {args.input_artifact} ...")
    with tempfile.TemporaryDirectory() as temp_dir:

        logger.info("Creating run")
        with wandb.init(project="nyc_airbnb", group='data_prep', job_type="basic_cleaning") as run:
            run.config.update(args)
            # Download the file streaming and write to open temp file
            local_path = wandb.use_artifact(f"{args.input_artifact}").file(temp_dir)
            df = pd.read_csv(local_path)
            logger.info("Successfully downloaded data")

            logger.info("Cleaning data")
            logger.info(f"Eliminating price outliers, n_rows before={df.shape[0]}")
            df = df[df['price'].between(args.min_price, args.max_price)]
            logger.info(f"Success, n_rows after={df.shape[0]}")

            logger.info("Changing data types")
            df['last_review'] = pd.to_datetime(df['last_review'])

            logger.info("Saving cleaned data")
            df.to_csv(f"{temp_dir}/clean_sample.csv", index=False)

            artifact = wandb.Artifact(
                args.output_artifact,
                type=args.output_type,
                description=args.output_description,
            )
            artifact.add_file(f"{temp_dir}/clean_sample.csv")
            run.log_artifact(artifact)
            logger.info("Data saved to W&B")
            logger.info("Success")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This is the step which cleans the data")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="minimum price",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="maximum price",
        required=True
    )

    args = parser.parse_args()

    go(args)
