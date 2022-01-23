import pandas as pd
import sys, gc, os, ast
from info_extractors import FirstInfoExtractor, MidInfoExtractor, FourthInfoExtractor


def main(all_files, file_suffix):
    '''
    :param all_files: An iterable or list of names of files to be processed.
    :return: Number of files processed and number of files not processed due to errors
    '''
    count = 0
    err_count = 0

    df1 = []
    df2 = []
    df3 = []
    df4 = []

    error_files = []

    for each_file in all_files:
        count += 1
        filepath = 'bids/txt/'+each_file

        try:
            temp1_extractor = FirstInfoExtractor(filepath, df1)
            temp1_extractor.extract_and_add()
            df1 = temp1_extractor.update_df()
            del temp1_extractor
            gc.collect()

            temp_mid_extractor = MidInfoExtractor(filepath, df2, df3)
            temp_mid_extractor.extract_and_add()
            df2 = temp_mid_extractor.update_second_df()
            df3 = temp_mid_extractor.update_third_df()
            del temp_mid_extractor
            gc.collect()

            temp4_extractor = FourthInfoExtractor(filepath, df4)
            temp4_extractor.extract_and_add()
            df4 = temp4_extractor.update_df()
            del temp4_extractor
            gc.collect()

        except Exception as e:
            err_count += 1
            error_files.append(each_file)
            continue

    if not os.path.exists('./Errors'):
        os.mkdir('./Errors')
    with open('./Errors/error_list_{0}.txt'.format(file_suffix), 'w') as f:
        err_summ = 'Total files scanned: {0} | Number of error files: {1}'.format(count, err_count)
        f.write("%s\n" % err_summ)
        for err in error_files:
            f.write("%s\n" % err)

    final_df1 = pd.DataFrame(df1)
    out_dir1 = './Folder1'
    if not os.path.exists(out_dir1):
        os.mkdir(out_dir1)
    result_name = '{0}/File_1_{1}.csv'.format(out_dir1, file_suffix)
    final_df1.to_csv(path_or_buf=result_name, index=False)
    del final_df1, df1
    gc.collect()

    final_df2 = pd.DataFrame(df2)
    out_dir2 = './Folder2'
    if not os.path.exists(out_dir2):
        os.mkdir(out_dir2)
    result_name = '{0}/File_2_{1}.csv'.format(out_dir2, file_suffix)
    final_df2.to_csv(path_or_buf=result_name, index=False)
    del final_df2, df2
    gc.collect()

    final_df3 = pd.DataFrame(df3)
    out_dir3 = './Folder3'
    if not os.path.exists(out_dir3):
        os.mkdir(out_dir3)
    result_name = '{0}/File_3_{1}.csv'.format(out_dir3, file_suffix)
    final_df3.to_csv(path_or_buf=result_name, index=False)
    del final_df3, df3
    gc.collect()

    final_df4 = pd.DataFrame(df4)
    out_dir4 = './Folder4'
    if not os.path.exists(out_dir4):
        os.mkdir(out_dir4)
    result_name = '{0}/File_4_{1}.csv'.format(out_dir4, file_suffix)
    final_df4.to_csv(path_or_buf=result_name, index=False)
    del final_df4, df4
    gc.collect()

    print(
        'Total files cleared: {0} | Number of error files: {1} | Percentage = {2:.2f}%'.format(
        count, err_count, (err_count/count)*100.0
        )
    )

if __name__ == "__main__":
    main(ast.literal_eval(sys.argv[1]), sys.argv[2])
