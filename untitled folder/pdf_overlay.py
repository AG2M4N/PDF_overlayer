import os
import sqlite3
import hashlib
import time
from pathlib import Path
import fitz  # PyMuPDF
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class PDFImageOverlayProcessor:
    def __init__(self, top_left_img, top_right_img, bottom_right_img,
                 center_img, bottom_left_img, right_margin_img,
                 input_folder, output_folder, db_path="processed_files.db"):
        self.top_left_img = top_left_img
        self.top_right_img = top_right_img
        self.bottom_right_img = bottom_right_img
        self.center_img = center_img
        self.bottom_left_img = bottom_left_img
        self.right_margin_img = right_margin_img
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.db_path = db_path
        
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                file_hash TEXT NOT NULL UNIQUE,
                input_path TEXT NOT NULL,
                output_path TEXT NOT NULL,
                processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
    
    def _calculate_file_hash(self, filepath):
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def _is_file_processed(self, file_hash):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT output_path FROM processed_files WHERE file_hash = ?', (file_hash,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    
    def _mark_file_processed(self, filename, file_hash, input_path, output_path):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO processed_files (filename, file_hash, input_path, output_path)
                VALUES (?, ?, ?, ?)
            ''', (filename, file_hash, str(input_path), str(output_path)))
            conn.commit()
        except sqlite3.IntegrityError:
            print(f"File {filename} already exists in database.")
        finally:
            conn.close()
    
    def _has_three_underscores(self, filename):
        name_without_ext = Path(filename).stem
        underscore_count = name_without_ext.count('_')
        return underscore_count == 3
    
    def overlay_images(self, input_pdf_path, output_pdf_path, 
                      top_left_config, top_right_config, bottom_right_config,
                      center_config, bottom_left_config, right_margin_config):
        pdf_doc = fitz.open(input_pdf_path)
        
        for page_num in range(len(pdf_doc)):
            page = pdf_doc[page_num]
            page_width = page.rect.width
            page_height = page.rect.height
            
            tl = top_left_config
            tl_width = tl['width'] * tl.get('scale_x', 1.0)
            tl_height = tl['height'] * tl.get('scale_y', 1.0)
            top_left_rect = fitz.Rect(
                tl['margin_x'], 
                tl['margin_y'], 
                tl['margin_x'] + tl_width, 
                tl['margin_y'] + tl_height
            )
            
            tr = top_right_config
            tr_width = tr['width'] * tr.get('scale_x', 1.0)
            tr_height = tr['height'] * tr.get('scale_y', 1.0)
            top_right_rect = fitz.Rect(
                page_width - tr['margin_x'] - tr_width,
                tr['margin_y'],
                page_width - tr['margin_x'],
                tr['margin_y'] + tr_height
            )
            
            br = bottom_right_config
            br_width = br['width'] * br.get('scale_x', 1.0)
            br_height = br['height'] * br.get('scale_y', 1.0)
            bottom_right_rect = fitz.Rect(
                page_width - br['margin_x'] - br_width,
                page_height - br['margin_y'] - br_height,
                page_width - br['margin_x'],
                page_height - br['margin_y']
            )
            
            c = center_config
            c_width = c['width'] * c.get('scale_x', 1.0)
            c_height = c['height'] * c.get('scale_y', 1.0)
            center_rect = fitz.Rect(
                (page_width - c_width) / 2 + c['offset_x'],
                (page_height - c_height) / 2 + c['offset_y'],
                (page_width + c_width) / 2 + c['offset_x'],
                (page_height + c_height) / 2 + c['offset_y']
            )
            
            bl = bottom_left_config
            bl_width = bl['width'] * bl.get('scale_x', 1.0)
            bl_height = bl['height'] * bl.get('scale_y', 1.0)
            bottom_left_rect = fitz.Rect(
                bl['margin_x'],
                page_height - bl['margin_y'] - bl_height,
                bl['margin_x'] + bl_width,
                page_height - bl['margin_y']
            )
            
            rm = right_margin_config
            rm_width = rm['width'] * rm.get('scale_x', 1.0)
            rm_height = rm['height'] * rm.get('scale_y', 1.0)
            right_margin_rect = fitz.Rect(
                page_width - rm['margin_x'] - rm_width,
                (page_height - rm_height) / 2 + rm['offset_y'],
                page_width - rm['margin_x'],
                (page_height + rm_height) / 2 + rm['offset_y']
            )
            
            page.insert_image(top_left_rect, filename=self.top_left_img)
            page.insert_image(top_right_rect, filename=self.top_right_img)
            page.insert_image(bottom_right_rect, filename=self.bottom_right_img)
            page.insert_image(center_rect, filename=self.center_img)
            page.insert_image(bottom_left_rect, filename=self.bottom_left_img)
            page.insert_image(right_margin_rect, filename=self.right_margin_img)
        
        pdf_doc.save(output_pdf_path)
        pdf_doc.close()
    
    def process_single_file(self, pdf_file, top_left_config, top_right_config, bottom_right_config,
                           center_config, bottom_left_config, right_margin_config):
        try:
            if not self._has_three_underscores(pdf_file.name):
                print(f"Skipping {pdf_file.name} (doesn't have 3 underscores)")
                return False
            
            file_hash = self._calculate_file_hash(pdf_file)
            
            existing_output = self._is_file_processed(file_hash)
            if existing_output:
                print(f"Skipping {pdf_file.name} (already processed)")
                return False
            
            output_path = self.output_folder / f"overlay_{pdf_file.name}"
            
            print(f"Processing: {pdf_file.name}...")
            self.overlay_images(pdf_file, output_path, 
                              top_left_config, top_right_config, bottom_right_config,
                              center_config, bottom_left_config, right_margin_config)
            
            self._mark_file_processed(pdf_file.name, file_hash, pdf_file, output_path)
            
            print(f"Completed: {output_path.name}\n")
            return True
            
        except Exception as e:
            print(f"Error processing {pdf_file.name}: {str(e)}\n")
            return False
    
    def process_folder(self, skip_processed=True, 
                      top_left_config=None, top_right_config=None, bottom_right_config=None,
                      center_config=None, bottom_left_config=None, right_margin_config=None):
        if top_left_config is None:
            top_left_config = {'width': 100, 'height': 100, 'margin_x': 10, 'margin_y': 10}
        if top_right_config is None:
            top_right_config = {'width': 100, 'height': 100, 'margin_x': 10, 'margin_y': 10}
        if bottom_right_config is None:
            bottom_right_config = {'width': 100, 'height': 100, 'margin_x': 10, 'margin_y': 10}
        if center_config is None:
            center_config = {'width': 100, 'height': 100, 'offset_x': 0, 'offset_y': 0}
        if bottom_left_config is None:
            bottom_left_config = {'width': 100, 'height': 100, 'margin_x': 10, 'margin_y': 10}
        if right_margin_config is None:
            right_margin_config = {'width': 100, 'height': 100, 'margin_x': 10, 'offset_y': 0}
        
        pdf_files = list(self.input_folder.glob("*.pdf"))
        
        if not pdf_files:
            print(f"No PDF files found in {self.input_folder}")
            return
        
        print(f"Found {len(pdf_files)} PDF file(s) to scan\n")
        
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        for pdf_file in pdf_files:
            if not self._has_three_underscores(pdf_file.name):
                print(f"Skipping {pdf_file.name} (doesn't have 3 underscores)")
                skipped_count += 1
                continue
            
            try:
                file_hash = self._calculate_file_hash(pdf_file)
                
                if skip_processed:
                    existing_output = self._is_file_processed(file_hash)
                    if existing_output:
                        print(f"Skipping {pdf_file.name} (already processed)")
                        skipped_count += 1
                        continue
                
                output_path = self.output_folder / f"overlay_{pdf_file.name}"
                
                print(f"Processing: {pdf_file.name}...")
                self.overlay_images(pdf_file, output_path, 
                                  top_left_config, top_right_config, bottom_right_config,
                                  center_config, bottom_left_config, right_margin_config)
                
                self._mark_file_processed(pdf_file.name, file_hash, pdf_file, output_path)
                
                print(f"Completed: {output_path.name}\n")
                processed_count += 1
                
            except Exception as e:
                print(f"Error processing {pdf_file.name}: {str(e)}\n")
                error_count += 1
        
        print("=" * 50)
        print(f"Processing complete!")
        print(f"Processed: {processed_count}")
        print(f"Skipped: {skipped_count}")
        print(f"Errors: {error_count}")
        print("=" * 50)
    
    def list_processed_files(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT filename, processed_date, output_path FROM processed_files ORDER BY processed_date DESC')
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            print("No processed files in database.")
            return
        
        print("\nProcessed Files:")
        print("=" * 80)
        for filename, date, output_path in results:
            print(f"{filename:<40} | {date} | {output_path}")
        print("=" * 80)


class PDFWatcherHandler(FileSystemEventHandler):
    def __init__(self, processor, top_left_config, top_right_config, bottom_right_config,
                 center_config, bottom_left_config, right_margin_config):
        self.processor = processor
        self.top_left_config = top_left_config
        self.top_right_config = top_right_config
        self.bottom_right_config = bottom_right_config
        self.center_config = center_config
        self.bottom_left_config = bottom_left_config
        self.right_margin_config = right_margin_config
        self.processing_files = set()
    
    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = Path(event.src_path)
        
        if file_path.suffix.lower() != '.pdf':
            return
        
        if str(file_path) in self.processing_files:
            return
        
        print(f"\nNew file detected: {file_path.name}")
        print("Waiting for file to be fully written...")
        time.sleep(2)
        
        self.processing_files.add(str(file_path))
        
        try:
            self.processor.process_single_file(
                file_path,
                self.top_left_config,
                self.top_right_config,
                self.bottom_right_config,
                self.center_config,
                self.bottom_left_config,
                self.right_margin_config
            )
        finally:
            self.processing_files.discard(str(file_path))


class PDFMonitor:
    def __init__(self, processor, top_left_config, top_right_config, bottom_right_config,
                 center_config, bottom_left_config, right_margin_config):
        self.processor = processor
        self.top_left_config = top_left_config
        self.top_right_config = top_right_config
        self.bottom_right_config = bottom_right_config
        self.center_config = center_config
        self.bottom_left_config = bottom_left_config
        self.right_margin_config = right_margin_config
        self.observer = None
    
    def start_monitoring(self):
        print("=" * 60)
        print("PDF OVERLAY PROCESSOR - CONTINUOUS MONITORING MODE")
        print("=" * 60)
        print(f"Watching folder: {self.processor.input_folder}")
        print(f"Filter: Files with exactly 3 underscores in name")
        print(f"Output folder: {self.processor.output_folder}")
        print("=" * 60)
        print("\nPress Ctrl+C to stop monitoring\n")
        
        event_handler = PDFWatcherHandler(
            self.processor,
            self.top_left_config,
            self.top_right_config,
            self.bottom_right_config,
            self.center_config,
            self.bottom_left_config,
            self.right_margin_config
        )
        
        self.observer = Observer()
        self.observer.schedule(
            event_handler,
            str(self.processor.input_folder),
            recursive=False
        )
        
        self.observer.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nStopping monitor...")
            self.observer.stop()
        
        self.observer.join()
        print("Monitor stopped successfully")


if __name__ == "__main__":
    # Get user's Downloads folder
    INPUT_FOLDER = Path.home() / "Downloads"
    OUTPUT_FOLDER = Path.home() / "Downloads" / "PDF_Overlayed"
    
    # Image paths (place these PNG files in same folder as exe)
    exe_dir = Path(__file__).parent if not getattr(sys, 'frozen', False) else Path(sys.executable).parent
    TOP_LEFT_IMAGE = str(exe_dir / "top_left.png")
    TOP_RIGHT_IMAGE = str(exe_dir / "top_right.png")
    BOTTOM_RIGHT_IMAGE = str(exe_dir / "bottom_right.png")
    CENTER_IMAGE = str(exe_dir / "center.png")
    BOTTOM_LEFT_IMAGE = str(exe_dir / "bottom_left.png")
    RIGHT_MARGIN_IMAGE = str(exe_dir / "right_margin.png")
    
    TOP_LEFT_CONFIG = {
        'width': 200, 'height': 200, 'margin_x': 20, 'margin_y': 20,
        'scale_x': 1.0, 'scale_y': 1.0
    }
    
    TOP_RIGHT_CONFIG = {
        'width': 200, 'height': 200, 'margin_x': 20, 'margin_y': 0,
        'scale_x': 1.0, 'scale_y': 1.0
    }
    
    BOTTOM_RIGHT_CONFIG = {
        'width': 350, 'height': 350, 'margin_x': 20, 'margin_y': -118,
        'scale_x': 1.0, 'scale_y': 1.0
    }
    
    CENTER_CONFIG = {
        'width': 300, 'height': 300, 'offset_x': 0, 'offset_y': 0,
        'scale_x': 2.0, 'scale_y': 2.0
    }
    
    BOTTOM_LEFT_CONFIG = {
        'width': 200, 'height': 200, 'margin_x': -7, 'margin_y': 0,
        'scale_x': 1.0, 'scale_y': 1.0
    }
    
    RIGHT_MARGIN_CONFIG = {
        'width': 150, 'height': 400, 'margin_x': -72, 'offset_y': 0,
        'scale_x': 1.0, 'scale_y': 2.5
    }
    
    processor = PDFImageOverlayProcessor(
        top_left_img=TOP_LEFT_IMAGE,
        top_right_img=TOP_RIGHT_IMAGE,
        bottom_right_img=BOTTOM_RIGHT_IMAGE,
        center_img=CENTER_IMAGE,
        bottom_left_img=BOTTOM_LEFT_IMAGE,
        right_margin_img=RIGHT_MARGIN_IMAGE,
        input_folder=INPUT_FOLDER,
        output_folder=OUTPUT_FOLDER
    )
    
    print("Processing existing files with 3 underscores...")
    processor.process_folder(
        skip_processed=True,
        top_left_config=TOP_LEFT_CONFIG,
        top_right_config=TOP_RIGHT_CONFIG,
        bottom_right_config=BOTTOM_RIGHT_CONFIG,
        center_config=CENTER_CONFIG,
        bottom_left_config=BOTTOM_LEFT_CONFIG,
        right_margin_config=RIGHT_MARGIN_CONFIG
    )
    
    print("\nStarting continuous monitoring mode...\n")
    monitor = PDFMonitor(
        processor,
        TOP_LEFT_CONFIG,
        TOP_RIGHT_CONFIG,
        BOTTOM_RIGHT_CONFIG,
        CENTER_CONFIG,
        BOTTOM_LEFT_CONFIG,
        RIGHT_MARGIN_CONFIG
    )
    monitor.start_monitoring()