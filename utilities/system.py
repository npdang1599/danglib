import os
import pickle
import json
import libtmux
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path


class FileHandler:
    """
    Các tiện ích xử lý file, bao gồm kiểm tra, tạo, đọc, ghi file với các định dạng khác nhau.
    """
    
    @staticmethod
    def isExists(file_path):
        """Kiểm tra xem file có tồn tại không
        
        Args:
            file_path (str): Đường dẫn đến file
            
        Returns:
            bool: True nếu file tồn tại, False nếu không
        """
        return Path(file_path).is_file()
    
    @staticmethod
    def walk_through_files(TARGET, ext="*"):
        """Duyệt qua tất cả các file trong thư mục với phần mở rộng được chỉ định
        
        Args:
            TARGET (str): Thư mục cần duyệt
            ext (str): Phần mở rộng của file để lọc, "*" cho tất cả các file
            
        Returns:
            list: Danh sách đường dẫn đến các file thỏa mãn điều kiện
        """
        lst = []
        for root, dirs, files in os.walk(TARGET):
            for file in files:
                if ext == "*" or file.endswith(ext):
                    lst.append(os.path.join(root, file))

        return lst
    
    @staticmethod
    def delete(file_path):
        """Xóa file
        
        Args:
            file_path (str): Đường dẫn đến file cần xóa
        """
        try:
            path = Path(file_path)
            path.unlink()
            print(f"File deleted: {file_path}")
        except FileNotFoundError:
            print("File does not exist.")
        except PermissionError:
            print("Permission denied to delete the file.")
        except Exception as e:
            print(f"An error occurred: {e}")

    @staticmethod
    def maybe_create_dir(path, verbose=0):
        """Tạo thư mục nếu chưa tồn tại
        
        Args:
            path (str): Đường dẫn thư mục cần tạo
            verbose (int): Mức độ hiển thị thông báo, 0 để không hiển thị
        """
        if not os.path.exists(path):
            os.makedirs(path)
            if verbose > 0:
                print(f"Created folder {path}")

    @staticmethod
    def write_pickle(path, data):
        """Ghi dữ liệu vào file pickle
        
        Args:
            path (str): Đường dẫn file pickle
            data: Dữ liệu cần ghi
        """
        with open(path, 'wb') as file:
            pickle.dump(data, file)
            
    @staticmethod
    def read_pickle(path):
        """Đọc dữ liệu từ file pickle
        
        Args:
            path (str): Đường dẫn file pickle
            
        Returns:
            object: Dữ liệu từ file pickle
        """
        with open(path, 'rb') as file:
            data = pickle.load(file)
        return data
    
    @staticmethod
    def write_parquet(file_path, df):
        """Ghi DataFrame vào file parquet
        
        Args:
            file_path (str): Đường dẫn file parquet
            df (pd.DataFrame): DataFrame cần ghi
        """
        df.to_parquet(file_path, compression='snappy')

    @staticmethod
    def read_parquet(file_path, columns=None):
        """Đọc DataFrame từ file parquet
        
        Args:
            file_path (str): Đường dẫn file parquet
            columns (list): Danh sách cột cần đọc, None để đọc tất cả
            
        Returns:
            pd.DataFrame: DataFrame từ file parquet
        """
        df = pd.read_parquet(file_path, columns=columns)
        return df
    
    @staticmethod
    def get_parquet_columns(path, substrs=None):
        """Lấy tên các cột từ file parquet
        
        Args:
            path (str): Đường dẫn file parquet
            substrs (list): Danh sách chuỗi con để lọc cột, None để lấy tất cả
            
        Returns:
            list: Danh sách tên cột
        """
        parquet_file = pq.ParquetFile(path)
        all_columns = parquet_file.schema.names
        if substrs is None:
            return all_columns
        if isinstance(substrs, str):
            substrs = [substrs]
        return [col for col in all_columns if any(substr in col for substr in substrs)]
    
    @staticmethod
    def load_json(file_path):
        """Đọc dữ liệu từ file JSON
        
        Args:
            file_path (str): Đường dẫn file JSON
            
        Returns:
            dict: Dữ liệu từ file JSON
        """
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data
    
    @staticmethod
    def save_json(file_path, data):
        """Ghi dữ liệu vào file JSON
        
        Args:
            file_path (str): Đường dẫn file JSON
            data: Dữ liệu cần ghi
            
        Returns:
            str: Đường dẫn file đã ghi
        """
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)    
        return file_path


def execute_cmd(cmd: str, print_result=True, wait_til_finish=True):
    """Thực thi lệnh hệ thống
    
    Args:
        cmd (str): Lệnh cần thực thi
        print_result (bool): In kết quả ra màn hình
        wait_til_finish (bool): Đợi đến khi lệnh hoàn thành
        
    Returns:
        str: Kết quả lệnh (nếu wait_til_finish=True)
        subprocess.Popen: Process (nếu wait_til_finish=False)
    """
    import subprocess, re, shlex
    cmd = cmd.replace("\n", "").strip().replace("\t", "")
    cmd = cmd.replace("\n", "").strip().replace("\t", "")
    cmd = re.sub(" +", " ", cmd)
    args = shlex.split(cmd)
    process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if not wait_til_finish:
        return process
    
    stdout, stderr = process.communicate()
    err = stderr.decode("utf-8")
    if len(err) > 0:
        print(f"{err}")
    res = stdout.decode("utf-8")
    if print_result:
        print(f"{res}")
    return re.sub(" +", " ", res)


def blockPrint():
    """Tắt đầu ra stdout"""
    import sys
    sys.stdout = open(os.devnull, 'w')


def enablePrint():
    """Bật lại đầu ra stdout"""
    import sys
    sys.stdout = sys.__stdout__


def check_run_with_interactive():
    """Kiểm tra xem code có đang chạy trong môi trường tương tác (Jupyter, IPython) không
    
    Returns:
        bool: True nếu đang chạy trong môi trường tương tác
    """
    import sys
    return any(
        arg in sys.argv[0] for arg in 
        ['ipykernel_launcher', 'ipykernel', '-c', 'jupyter', 'vscode']
    )


def show_ram_usage_mb():
    """Hiển thị lượng RAM đang sử dụng bởi process hiện tại
    
    Returns:
        float: Lượng RAM sử dụng (MB)
    """
    import os
    import psutil
    pid = os.getpid()
    python_process = psutil.Process(pid)
    memoryUse = round(python_process.memory_info()[0] / 2. ** 20, 2)  # memory use in MB
    return memoryUse


def divide_lst_to_blocks(target_lst, block_len):
    """Chia danh sách thành các khối có kích thước cố định
    
    Args:
        target_lst (list): Danh sách cần chia
        block_len (int): Kích thước mỗi khối
        
    Returns:
        list: Danh sách các khối
    """
    block_ls = [target_lst[i:i+block_len] for i in range(0, len(target_lst), block_len)]
    return block_ls


def hash_sha256(value):
    """Tính hash SHA-256 của một giá trị
    
    Args:
        value (str or bytes): Giá trị cần hash
        
    Returns:
        str: Chuỗi hex của hash SHA-256
    """
    import hashlib
    # Chuyển thành bytes nếu đầu vào là chuỗi
    value_bytes = value.encode('utf-8') if isinstance(value, str) else value
    # Tính hash bằng SHA-256
    hash_object = hashlib.sha256(value_bytes)
    return hash_object.hexdigest()  # Trả về dạng chuỗi hex


def clean_plasma_key(key, db):
    """Xóa key trong Apache Plasma
    
    Args:
        key: Key cần xóa
        db: Cơ sở dữ liệu plasma
        
    Returns:
        key: Key đã xóa
    """
    from danglib.lazy_core import gen_plasma_functions
    _, disconnect, psave, pload = gen_plasma_functions(db=db)
    try:
        psave(key, None)
        return key
    finally:
        disconnect()


class Tmuxlib:
    """Tiện ích cho việc làm việc với tmux"""

    @staticmethod
    def init_session(session_name):
        """Khởi tạo session tmux
        
        Args:
            session_name (str): Tên session
            
        Returns:
            libtmux.Session: Session tmux
        """
        server = libtmux.Server()
        session = server.find_where({'session_name': session_name})
        return session

    @staticmethod
    def terminate_pane_process(pane):
        """Kết thúc process đang chạy trong pane
        
        Args:
            pane (libtmux.Pane): Pane tmux
        """
        pane.send_keys("C-c")

    @staticmethod
    def terminate_and_run_cmd_pane(pane, cmd, start_dir=None, conda_env=None):
        """Kết thúc process hiện tại và chạy lệnh mới trong pane
        
        Args:
            pane (libtmux.Pane): Pane tmux
            cmd (str): Lệnh cần chạy
            start_dir (str): Thư mục bắt đầu
            conda_env (str): Môi trường conda
        """
        Tmuxlib.terminate_pane_process(pane)
        
        if start_dir is not None:
            pane.send_keys(f"cd {start_dir}")
            
        if conda_env is not None:
            pane.send_keys(f"conda activate {conda_env}")
            
        pane.send_keys(f"alias cmd='{cmd}'")
        pane.send_keys("cmd")

    @staticmethod
    def check_right_window_format(window, n_panes, idx_list):
        """Kiểm tra định dạng cửa sổ tmux
        
        Args:
            window (libtmux.Window): Cửa sổ tmux
            n_panes (int): Số lượng pane mong muốn
            idx_list (list): Danh sách ID pane mong muốn
            
        Returns:
            bool: True nếu định dạng đúng
        """
        is_unchange_npane = len(window.panes) == n_panes
        is_unchange_size = all([idx in idx_list for idx in [p.pane_id for p in window.panes]])
        
        return is_unchange_npane

    @staticmethod
    def restart_db_worker(session_name, window_index, pane_index, 
                         start_dir, cmd, conda_env=None, notification=True):
        """Khởi động lại worker cơ sở dữ liệu
        
        Args:
            session_name (str): Tên session tmux
            window_index (int): Chỉ số cửa sổ
            pane_index (int): Chỉ số pane
            start_dir (str): Thư mục bắt đầu
            cmd (str): Lệnh khởi động worker
            conda_env (str): Môi trường conda
            notification (bool): Gửi thông báo hay không
            
        Returns:
            bool: True nếu khởi động lại thành công
        """
        try:
            ss = Tmuxlib.init_session(session_name=session_name)
            if not ss:
                print(f"Session {session_name} not found")
                return False
                
            wd = ss.windows[window_index]
            pane = wd.panes[pane_index]
            
            Tmuxlib.terminate_and_run_cmd_pane(
                pane, 
                start_dir=start_dir,
                cmd=cmd,
                conda_env=conda_env
            )
            
            if notification:
                try:
                    from danglib.chatbots.viberbot import create_f5bot
                    F5bot = create_f5bot()
                    F5bot.send_viber(msg=f"Worker in {session_name} has been restarted!")
                except ImportError:
                    print(f"Worker in {session_name} has been restarted!")
                    
            return True
            
        except Exception as e:
            print(f"Error restarting worker: {str(e)}")
            return False