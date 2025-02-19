import requests
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
import os
import time
import re
import argparse
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class DeputiesDataCollector:
    """Collects and organizes data from the Brazilian Chamber of Deputies."""

    BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
    WEBSITE_URL = "https://www.camara.leg.br/deputados"

    def __init__(
        self,
        legislature: int = 57,
        max_workers: int = 10,
        output_dir: str = "/bronze",
        sleep_time: float = 1.33,
    ):
        """Initialize the data collector."""
        self.legislature = legislature
        self.max_workers = max_workers
        self.output_dir = output_dir
        self.sleep_time = sleep_time

        # Initialize sessions
        self.api_session = requests.Session()
        self.web_session = requests.Session()

        # Set headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.api_session.headers.update(headers)
        self.web_session.headers.update(headers)

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

    def _make_request(
        self, url: str, params: Dict = None, session: Optional[requests.Session] = None
    ) -> Dict:
        """Make HTTP request with error handling."""
        session = session or self.api_session
        try:
            response = session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {url}: {str(e)}")
            raise

    def get_deputies_table(self) -> pd.DataFrame:
        """Collect deputies data and create the deputies dimension table."""
        deputies_data = []
        page = 1
        params = {
            "idLegislatura": self.legislature,
            "ordenarPor": "nome",
            "itens": 1000,
        }

        while True:
            params["pagina"] = page
            try:
                data = self._make_request(f"{self.BASE_URL}/deputados", params=params)

                if not data["dados"]:
                    break

                deputies_data.extend(data["dados"])
                total_items = int(data.get("links", [{}])[0].get("total", 0))

                logger.info(
                    f"Fetched page {page} of deputies. Total items: {total_items}"
                )

                if len(deputies_data) >= total_items:
                    break

                page += 1

            except Exception as e:
                logger.error(f"Error fetching deputies page {page}: {str(e)}")
                break

        if not deputies_data:
            logger.error("No deputies data collected!")
            return pd.DataFrame()

        df = pd.DataFrame(deputies_data)

        deputies_dim = df[
            [
                "id",
                "nome",
                "siglaPartido",
                "siglaUf",
                "email",
                "urlFoto",
                "idLegislatura",
            ]
        ].copy()

        deputies_dim = deputies_dim.rename(
            columns={
                "id": "deputy_id",
                "nome": "name",
                "siglaPartido": "party",
                "siglaUf": "state",
                "urlFoto": "photo_url",
            }
        )

        return deputies_dim.drop_duplicates(subset=["deputy_id"])

    def get_expenses_table(self, deputy_id: int) -> pd.DataFrame:
        """Get expenses data for a specific deputy."""
        try:
            params = {"itens": 1}
            url = f"{self.BASE_URL}/deputados/{deputy_id}/despesas"
            response = self.api_session.get(url, params=params)

            if response.status_code != 200:
                return pd.DataFrame()

            total_items = int(response.headers.get("x-total-count", 0))
            total_pages = (total_items + 99) // 100

            logger.info(
                f"Fetching {total_pages} pages of expenses for deputy {deputy_id}"
            )

            all_expenses = []
            for page in range(1, total_pages + 1):
                try:
                    params = {
                        "pagina": page,
                        "itens": 1000,
                        "ordenarPor": "dataDocumento",
                    }
                    data = self._make_request(url, params=params)
                    all_expenses.extend(data["dados"])
                except Exception as e:
                    logger.error(
                        f"Error fetching expenses page {page} for deputy {deputy_id}: {str(e)}"
                    )
                    continue

            if not all_expenses:
                return pd.DataFrame()

            expenses_df = pd.DataFrame(all_expenses)
            expenses_df["deputy_id"] = deputy_id

            # Convert date and extract month/year
            expenses_df["dataDocumento"] = pd.to_datetime(expenses_df["dataDocumento"])

            # Select relevant columns
            expenses_fact = expenses_df[
                [
                    "deputy_id",
                    "valorDocumento",
                    "dataDocumento",
                    "valorLiquido",
                    "mes",
                    "ano",
                    "tipoDespesa",
                    "tipoDocumento",
                    "cnpjCpfFornecedor",
                    "nomeFornecedor",
                    "parcela",
                ]
            ].copy()

            # Convert valorDocumento to float
            expenses_fact["valorLiquido"] = pd.to_numeric(
                expenses_fact["valorDocumento"], errors="coerce"
            )

            return expenses_fact

        except Exception as e:
            logger.error(f"Error processing expenses for deputy {deputy_id}: {str(e)}")
            return pd.DataFrame()

    def batch_scrape_attendance(
        self, deputy_ids: List[int], year: int = 2024
    ) -> pd.DataFrame:
        """Batch scrapes attendance data for multiple deputies in a given year."""
        results = []
        timestamp = datetime.now()

        logger.info(
            f"Scraping attendance data for {len(deputy_ids)} deputies in {year}"
        )

        for deputy_id in tqdm(deputy_ids, desc="Processing deputies"):
            try:
                url = f"{self.WEBSITE_URL}/{deputy_id}?ano={year}"
                time.sleep(self.sleep_time)

                response = self.web_session.get(url)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")

                # Find plenary attendance section
                plenary_section = None
                for section in soup.find_all("section", class_="presencas__section"):
                    heading = section.find("h4", class_="presencas__section-heading")
                    if heading and "Presença em Plenário" in heading.text:
                        plenary_section = section
                        break

                if not plenary_section:
                    logger.warning(
                        f"No plenary attendance section found for deputy {deputy_id}"
                    )
                    results.append(
                        {
                            "deputy_id": deputy_id,
                            "year": year,
                            "timestamp": timestamp,
                            "presencas": 0,
                            "ausencias_justificadas": 0,
                            "ausencias_nao_justificadas": 0,
                        }
                    )
                    continue

                attendance_data = {}
                attendance_items = plenary_section.find_all(
                    "li", class_="presencas__data"
                )

                for item in attendance_items:
                    label = item.find("span", class_="presencas__label").text.strip()
                    value_text = item.find("span", class_="presencas__qtd").text.strip()
                    try:
                        value = int(re.search(r"\d+", value_text).group())
                    except Exception:
                        value = 0

                    label_lower = label.lower()
                    if "presença" in label_lower and "ausência" not in label_lower:
                        attendance_data["presencas"] = value
                    elif (
                        "ausências justificadas" in label_lower
                        or "ausencias justificadas" in label_lower
                    ):
                        attendance_data["ausencias_justificadas"] = value
                    elif (
                        "ausências não justificadas" in label_lower
                        or "ausencias nao justificadas" in label_lower
                    ):
                        attendance_data["ausencias_nao_justificadas"] = value

                results.append(
                    {
                        "deputy_id": deputy_id,
                        "year": year,
                        "timestamp": timestamp,
                        "presencas": attendance_data.get("presencas", 0),
                        "ausencias_justificadas": attendance_data.get(
                            "ausencias_justificadas", 0
                        ),
                        "ausencias_nao_justificadas": attendance_data.get(
                            "ausencias_nao_justificadas", 0
                        ),
                    }
                )
                logger.info(f"Collected attendance data for deputy {deputy_id}")

            except Exception as e:
                logger.error(
                    f"Error collecting attendance data for deputy {deputy_id}: {str(e)}"
                )
                results.append(
                    {
                        "deputy_id": deputy_id,
                        "year": year,
                        "timestamp": timestamp,
                        "presencas": 0,
                        "ausencias_justificadas": 0,
                        "ausencias_nao_justificadas": 0,
                    }
                )

        # Create DataFrame
        df = pd.DataFrame(results)

        # Add percentage calculations
        if not df.empty:
            df["total_dias"] = (
                df["presencas"]
                + df["ausencias_justificadas"]
                + df["ausencias_nao_justificadas"]
            )
            df["taxa_presenca"] = (df["presencas"] / df["total_dias"] * 100).round(2)

        return df

    def get_propositions_data(self, deputy_id: int, year: int) -> Dict:
        """
        Get propositions data for a deputy for a specific year.
        Returns a dict with deputy_id, proposition_count, and a list of ementa texts.
        """
        try:
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            base_url = f"{self.BASE_URL}/proposicoes"
            params = {
                "idDeputadoAutor": deputy_id,
                "dataApresentacaoInicio": start_date,
                "dataApresentacaoFim": end_date,
                "itens": 1,
                "ordem": "ASC",
                "ordenarPor": "id",
            }
            response = self.api_session.get(base_url, params=params)
            response.raise_for_status()
            total_count = int(response.headers.get("x-total-count", 0))
            ementas = []
            if total_count > 0:
                total_pages = (total_count + 999) // 1000
                all_props = []
                for page in range(1, total_pages + 1):
                    params_page = {
                        "idDeputadoAutor": deputy_id,
                        "dataApresentacaoInicio": start_date,
                        "dataApresentacaoFim": end_date,
                        "pagina": page,
                        "itens": 1000,
                        "ordem": "ASC",
                        "ordenarPor": "id",
                    }
                    page_response = self.api_session.get(base_url, params=params_page)
                    page_response.raise_for_status()
                    data = page_response.json()
                    all_props.extend(data.get("dados", []))
                for prop in all_props:
                    ementa = prop.get("ementa")
                    if ementa:
                        ementas.append(ementa)
            return {
                "deputy_id": deputy_id,
                "proposition_count": total_count,
                "ementas": ementas,
            }
        except Exception as e:
            logger.error(
                f"Error fetching propositions for deputy {deputy_id} in year {year}: {e}"
            )
            return {"deputy_id": deputy_id, "proposition_count": 0, "ementas": []}

    def save_deputies_data(self, year: int = 2024) -> pd.DataFrame:
        """Run and save deputies data to CSV."""
        deputies_dim = self.get_deputies_table()
        timestamp = datetime.now().strftime("%Y%m%d")
        file_path = os.path.join(self.output_dir, f"deputies_{year}_{timestamp}.csv")
        deputies_dim.to_csv(file_path, index=False)
        logger.info(f"Saved deputies data to {file_path}")
        return deputies_dim

    def save_expenses_data(
        self, deputy_ids: Optional[List[int]] = None, year: int = 2024
    ) -> pd.DataFrame:
        """Run and save expenses data to CSV."""
        if deputy_ids is None:
            deputies_dim = self.get_deputies_table()
            deputy_ids = deputies_dim["deputy_id"].tolist()
        all_expenses = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            expenses_dfs = list(executor.map(self.get_expenses_table, deputy_ids))
            for df in expenses_dfs:
                if not df.empty:
                    all_expenses.append(df)
        expenses_fact = (
            pd.concat(all_expenses, ignore_index=True)
            if all_expenses
            else pd.DataFrame()
        )
        timestamp = datetime.now().strftime("%Y%m%d")
        file_path = os.path.join(self.output_dir, f"expense_{year}_{timestamp}.csv")
        expenses_fact.to_csv(file_path, index=False)
        logger.info(f"Saved expenses data to {file_path}")
        return expenses_fact

    def save_attendance_data(
        self, deputy_ids: Optional[List[int]] = None, year: int = 2024
    ) -> pd.DataFrame:
        """Run and save attendance data to CSV (non-batched version)."""
        if deputy_ids is None:
            deputies_dim = self.get_deputies_table()
            deputy_ids = deputies_dim["deputy_id"].tolist()
        attendance_dim = self.batch_scrape_attendance(deputy_ids, year)
        timestamp = datetime.now().strftime("%Y%m%d")
        file_path = os.path.join(self.output_dir, f"attendance_{year}_{timestamp}.csv")
        attendance_dim.to_csv(file_path, index=False)
        logger.info(f"Saved attendance data to {file_path}")
        return attendance_dim

    def save_attendance_data_batched(
        self,
        deputy_ids: Optional[List[int]] = None,
        year: int = 2024,
        batch_size: int = 50,
        pause_time: int = 10,
    ) -> pd.DataFrame:
        """
        Run and save attendance data in batches of deputy IDs.
        Processes deputy IDs in batches of batch_size, sleeps for pause_time seconds between batches,
        and appends the results to the CSV.
        """
        if deputy_ids is None:
            deputies_dim = self.get_deputies_table()
            deputy_ids = deputies_dim["deputy_id"].tolist()

        timestamp = datetime.now().strftime("%Y%m%d")
        file_path = os.path.join(self.output_dir, f"attendance_{year}_{timestamp}.csv")
        result_df = pd.DataFrame()

        # Process in batches
        for i in range(0, len(deputy_ids), batch_size):
            batch_ids = deputy_ids[i : i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}: {batch_ids}")
            batch_df = self.batch_scrape_attendance(batch_ids, year)
            # Append to CSV (create new file if it doesn't exist)
            if os.path.exists(file_path):
                batch_df.to_csv(file_path, mode="a", index=False, header=False)
            else:
                batch_df.to_csv(file_path, index=False)
            result_df = pd.concat([result_df, batch_df], ignore_index=True)
            # Sleep if there are more batches to process
            if i + batch_size < len(deputy_ids):
                logger.info(f"Sleeping for {pause_time} seconds between batches")
                time.sleep(pause_time)

        logger.info(f"Saved batched attendance data to {file_path}")
        return result_df

    def save_propositions_data(
        self, deputy_ids: Optional[List[int]] = None, year: int = 2024
    ) -> pd.DataFrame:
        """Run and save propositions data to CSV."""
        if deputy_ids is None:
            deputies_dim = self.get_deputies_table()
            deputy_ids = deputies_dim["deputy_id"].tolist()
        all_propositions = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(
                executor.map(
                    lambda dep_id: self.get_propositions_data(dep_id, year), deputy_ids
                )
            )
            for res in results:
                all_propositions.append(res)
        propositions_dim = pd.DataFrame(all_propositions)
        timestamp = datetime.now().strftime("%Y%m%d")
        file_path = os.path.join(self.output_dir, f"proposition_{year}_{timestamp}.csv")
        propositions_dim.to_csv(file_path, index=False)
        logger.info(f"Saved propositions data to {file_path}")
        return propositions_dim

    def build_data_collection(
        self, year: int = 2024
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Collect all data tables and save them to CSV."""
        logger.info(f"Starting data collection for legislature {self.legislature}...")
        deputies_dim = self.get_deputies_table()
        if deputies_dim.empty:
            logger.error("No deputies found. Aborting data collection.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        deputy_ids = deputies_dim["deputy_id"].tolist()

        # Collect expenses
        logger.info(f"Collecting expenses for {len(deputy_ids)} deputies...")
        expenses_fact = self.save_expenses_data(deputy_ids, year)

        # Collect attendance data (non-batched version)
        attendance_dim = self.save_attendance_data(deputy_ids, year)

        # Collect propositions data
        propositions_dim = self.save_propositions_data(deputy_ids, year)

        # Save deputies data (if not already saved)
        self.save_deputies_data(year)

        logger.info(f"""Data collection completed:
            - Deputies: {len(deputies_dim)}
            - Expense records: {len(expenses_fact)}
            - Attendance records: {len(attendance_dim)}
            - Proposition records: {len(propositions_dim)}
            """)
        return deputies_dim, expenses_fact, attendance_dim, propositions_dim


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run deputies data collection tasks separately."
    )
    parser.add_argument(
        "task",
        choices=[
            "deputies",
            "expenses",
            "attendance",
            "attendance_batched",
            "propositions",
            "all",
        ],
        help="Which task to run",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Year to filter attendance/propositions data",
    )
    args = parser.parse_args()

    data_collector = DeputiesDataCollector(
        legislature=57, output_dir="data/bronze", sleep_time=2
    )

    if args.task == "deputies":
        data_collector.save_deputies_data(year=args.year)
    elif args.task == "expenses":
        data_collector.save_expenses_data(year=args.year)
    elif args.task == "attendance":
        data_collector.save_attendance_data(year=args.year)
    elif args.task == "attendance_batched":
        data_collector.save_attendance_data_batched(year=args.year)
    elif args.task == "propositions":
        data_collector.save_propositions_data(year=args.year)
    elif args.task == "all":
        data_collector.build_data_collection(year=args.year)
    else:
        print(
            "Invalid task. Choose from deputies, expenses, attendance, attendance_batched, propositions, or all."
        )
