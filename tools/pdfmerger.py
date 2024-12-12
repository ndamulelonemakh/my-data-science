#!/usr/bin/env python3
"""
Generic PDF Merger Utility

Merges multiple PDF files in a directory with optional date-based sorting.
Supports custom sorting methods and flexible filename patterns.
"""

import os
import re
import logging
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Optional, Callable
from dataclasses import dataclass
from pathlib import Path

from PyPDF2 import PdfReader, PdfWriter

class SortMethod(Enum):
    """Available sorting methods for PDF files."""
    NONE = "none"
    NAME = "name"
    DATE_MODIFIED = "modified"
    DATE_FROM_FILENAME = "filename_date"

@dataclass
class PDFMergerConfig:
    """Configuration settings for PDF merger."""
    source_dir: Path
    output_filename: str = "merged.pdf"
    sort_method: SortMethod = SortMethod.NONE
    # Optional date pattern in regex format
    date_pattern: str = r".*-(\d{2})-(\d{2})-(\d{4})\.pdf$"
    date_format: str = "%m-%d-%Y"
    file_pattern: str = ".pdf"
    recursive: bool = False

class PDFMerger:
    """Handles merging of PDF files with flexible sorting options."""
    
    def __init__(self, config: PDFMergerConfig):
        self.config = config
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure logging for the application."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def _parse_date_from_filename(self, filename: str) -> Optional[datetime]:
        """
        Extract date from filename using regex pattern.
        
        Args:
            filename: Name of the PDF file
            
        Returns:
            datetime object if successful, None if parsing fails
        """
        try:
            match = re.match(self.config.date_pattern, filename)
            if match:
                # Extract captured groups and construct date string
                date_parts = match.groups()
                date_str = "-".join(date_parts)
                return datetime.strptime(date_str, self.config.date_format)
        except (ValueError, IndexError) as e:
            logging.debug(f"Failed to parse date from {filename}: {e}")
        return None

    def _get_sorting_key(self) -> Callable:
        """
        Get the appropriate sorting key based on configuration.
        
        Returns:
            Callable that serves as key function for sorting
        """
        if self.config.sort_method == SortMethod.NAME:
            return lambda x: x.name.lower()
        elif self.config.sort_method == SortMethod.DATE_MODIFIED:
            return lambda x: x.stat().st_mtime
        elif self.config.sort_method == SortMethod.DATE_FROM_FILENAME:
            def date_key(path):
                date = self._parse_date_from_filename(path.name)
                return date if date else datetime.max
            return date_key
        return lambda x: 0  # No sorting

    def _get_pdf_files(self) -> List[Path]:
        """
        Get all PDF files from source directory.
        
        Returns:
            List of PDF file paths
        """
        pattern = f"**/*{self.config.file_pattern}" if self.config.recursive else f"*{self.config.file_pattern}"
        files = list(self.config.source_dir.glob(pattern))
        
        if not files:
            logging.warning("No PDF files found")
            return []

        # Sort files if method specified
        if self.config.sort_method != SortMethod.NONE:
            sort_key = self._get_sorting_key()
            files.sort(key=sort_key)
            
        return files

    def merge_pdfs(self) -> Optional[Path]:
        """
        Merge PDF files according to specified configuration.
        
        Returns:
            Path to the merged PDF file if successful, None otherwise
        """
        try:
            files = self._get_pdf_files()
            if not files:
                logging.error("No valid PDF files found")
                return None

            writer = PdfWriter()
            for file_path in files:
                logging.info(f"Processing: {file_path.name}")
                try:
                    with open(file_path, 'rb') as pdf_file:
                        reader = PdfReader(pdf_file)
                        for page in reader.pages:
                            writer.add_page(page)
                except Exception as e:
                    logging.error(f"Error processing {file_path.name}: {e}")
                    continue

            output_path = self.config.source_dir / self.config.output_filename
            with open(output_path, 'wb') as output_file:
                writer.write(output_file)

            logging.info(f"Successfully merged PDFs to: {output_path}")
            return output_path

        except Exception as e:
            logging.error(f"Failed to merge PDFs: {e}")
            return None

def main():
    """Example usage of the PDF merger utility."""
    # Example with different sorting methods
    config_examples = [
        # No sorting
        PDFMergerConfig(
            source_dir=Path("/path/to/pdfs"),
            sort_method=SortMethod.NONE
        ),
        # Sort by filename
        PDFMergerConfig(
            source_dir=Path("/path/to/pdfs"),
            sort_method=SortMethod.NAME
        ),
        # Sort by modification date
        PDFMergerConfig(
            source_dir=Path("/path/to/pdfs"),
            sort_method=SortMethod.DATE_MODIFIED
        ),
        # Sort by date in filename with custom pattern
        PDFMergerConfig(
            source_dir=Path("/path/to/pdfs"),
            sort_method=SortMethod.DATE_FROM_FILENAME,
            date_pattern=r"(\d{4})-(\d{2})-(\d{2})",  # Different date pattern example
            date_format="%Y-%m-%d"
        )
    ]
    
    # Use the first example
    merger = PDFMerger(config_examples[0])
    result = merger.merge_pdfs()
    
    if result:
        print(f"Merged PDF saved to: {result}")
    else:
        print("Failed to merge PDFs. Check logs for details.")

if __name__ == "__main__":
    main()
