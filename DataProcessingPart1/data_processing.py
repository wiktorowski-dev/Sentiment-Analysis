from DataProcessingPart1.re_sizer import ReSizerFiles
from DataProcessingPart1.functions import *
import multiprocessing
import time


if __name__ == '__main__':
    # Declare re sizer function
    re_sizer = ReSizerFiles.re_sizer
    #
    tt = time.time()
    #

    # Slicer variables
    input_path_to_large_file = r'D:\s_a\All_Amazon_Review.json'
    output_file_path_post_slicer = r'D:\s_a\file_{}'

    # .json to .csv formatter
    cores = 5
    input_file_path = r'D:\s_a\file_{}'

    # Duplication remover variables
    input_file_name = r'D:\s_a\file_large_{}'
    output_file_name_neg = r'D:\s_a\file_large_n_{}'
    output_file_name_pos = r'D:\s_a\file_large_p_{}'

    # Re sizer variables
    output_positive_files = r'D:\s_a\p_{}'
    output_negative_files = r'D:\s_a\n_{}'

    slicer(input_path_to_large_file, output_file_path_post_slicer, 1200000)

    files_name = [input_file_path.format(x) for x in range(count_files_in_dir(input_file_path))]
    with multiprocessing.Pool(cores) as p:
        p.map(json_to_csv, files_name)
    re_sizer(input_file_name=input_file_path,
             output_file_name=input_file_name,
             max_length_of_single_output=8000000,
             auto_file_remover=True,
             auto_detect_files_in_dir=True,
             files_in_folder=None)

    duplicate_remover(input_file_name,
                      output_file_name_neg,
                      output_file_name_pos,
                      files_in_folder=count_files_in_dir(input_file_name),
                      auto_remove_files=True)

    re_sizer(input_file_name=output_file_name_neg,
             output_file_name=output_negative_files,
             max_length_of_single_output=1000000,
             auto_file_remover=True,
             auto_detect_files_in_dir=True,
             files_in_folder=None)

    re_sizer(input_file_name=output_file_name_pos,
             output_file_name=output_positive_files,
             max_length_of_single_output=1000000,
             max_amount_output_of_files=count_files_in_dir(output_negative_files),
             total_length_of_output=count_rows_in_files(output_negative_files,
                                                        count_files_in_dir(output_negative_files)),
             auto_file_remover=True,
             auto_detect_files_in_dir=True,
             files_in_folder=None)

    o = time.time() - tt
    print(o)

