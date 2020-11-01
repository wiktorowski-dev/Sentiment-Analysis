import os
import re
import pandas as pd


class ReSizerFiles(object):
    def __init__(self):
        super(ReSizerFiles, self).__init__()

    @staticmethod
    def re_sizer(input_file_name: str, output_file_name: str,
                 max_length_of_single_output: int, max_amount_output_of_files=float('inf'),
                 total_length_of_output: int = float('inf'),
                 auto_file_remover: bool = False, auto_detect_files_in_dir: bool = False, files_in_folder: int = None):

        if not auto_detect_files_in_dir and not files_in_folder:
            raise Exception("Declare files_in_folder or auto_detect_files_in_dir")

        if auto_detect_files_in_dir and not files_in_folder:
            files_in_folder = [x for x in os.listdir(os.path.split(input_file_name)[0]) if
                               os.path.split(input_file_name)[-1].replace("{", '').replace("}", '') in x
                               and os.path.split(input_file_name)[-1].replace("{", '').replace("}", '') ==
                               re.sub(r'\d', '', x)].__len__()

        files_name_lest_to_remove = [input_file_name.format(x) for x in range(files_in_folder)]

        index_output_file = 0
        df_out = pd.DataFrame()
        df = pd.DataFrame()

        # Iterate over each file declared as input

        for i in range(files_in_folder):

            if files_in_folder == 0:
                df.drop(df.index, inplace=True)
                df_out.drop(df_out.index, inplace=True)
                # Break after break in while loop
                break

            if i == 21:
                print(123)
            print("input_file_{}| output_file_{}".format(i, index_output_file))

            df = pd.read_csv(input_file_name.format(i))

            length = df_out.__len__() + df.__len__()

            # Check if file is enough large to save it
            if length >= max_length_of_single_output or total_length_of_output <= max_length_of_single_output:
                # Calculate left where to cut data frame to get equal value to declared one
                to_cut = df.__len__() - (length - max_length_of_single_output)

                if total_length_of_output < max_length_of_single_output:
                    to_cut = total_length_of_output
                    if to_cut == 0:
                        df.drop(df.index, inplace=True)
                        df_out.drop(df_out.index, inplace=True)
                        break
                    if length - max_length_of_single_output < 0:
                        to_cut = total_length_of_output - df_out.__len__()

                df_out = pd.concat([df_out, df[:to_cut]])

                df_out.to_csv(output_file_name.format(index_output_file), mode='a', index=False)
                total_length_of_output -= df_out.__len__()

                # Overwrite file data frame and remove already saved data
                df_out = pd.concat([df[to_cut:]])
                df.drop(df.index, inplace=True)

                if max_amount_output_of_files == index_output_file \
                        or total_length_of_output <= 0:
                    df.drop(df.index, inplace=True)
                    df_out.drop(df_out.index, inplace=True)
                    # remove loop if get declared amount of file in output
                    break

                index_output_file += 1

                # Loop for deal with larger file separated to few smaller one
                while True:
                    # Is size of df_positive is much larger than limit
                    if df_out.__len__() > max_length_of_single_output:
                        print("input_file_{}| output_file_{}".format(i, index_output_file))
                        if total_length_of_output < max_length_of_single_output:
                            max_length_of_single_output = total_length_of_output

                        df_out[:max_length_of_single_output].to_csv(output_file_name.format(index_output_file), mode='a',
                                                                    index=False)

                        total_length_of_output -= df_out[:max_length_of_single_output].__len__()

                        df_out = df_out[max_length_of_single_output:]

                        if max_amount_output_of_files <= index_output_file or \
                                total_length_of_output == 0:
                            # remove loop if get declared amount of file in output
                            files_in_folder = 0
                            break

                        index_output_file += 1


                    else:
                        break
            else:
                df_out = pd.concat([df_out, df])
                df.drop(df.index, inplace=True)

            if auto_file_remover:
                os.remove(input_file_name.format(i))
                files_name_lest_to_remove.remove(input_file_name.format(i))

        if df_out.__len__() != 0:
            df_out.to_csv(output_file_name.format(index_output_file), mode='a', index=False)

        if auto_file_remover:
            for file_to_remove in files_name_lest_to_remove:
                os.remove(file_to_remove)
