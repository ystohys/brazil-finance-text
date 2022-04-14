import pandas as pd
import sys, gc, os, ast
from info_extractors import FirstInfoExtractor, MidInfoExtractor, FourthInfoExtractor


def main(all_files, file_suffix, folder_pathway):
    '''
    :param all_files: An iterable or list of names of files to be processed.
    :param file_suffix: The label appended to the end of each processed file.
    :param folder_pathway: The path to the folder where all txt files are kept.
    :return: Number of files processed and number of files not processed due to errors
    '''
    grand_count = 0 # Total number of files went through
    count_one, count_two, count_three, count_four, count_five = 0, 0, 0, 0, 0
    err_count_one, err_count_two, err_count_three, err_count_four, err_count_five = 0, 0, 0, 0, 0

    df1 = []
    df2 = []
    df3 = []
    df4 = []
    df5 = []

    error_files_one = []
    error_files_two = []
    error_files_three = []
    error_files_four = []
    error_files_five = []

    for each_file in all_files:
        filepath = './{0}/{1}'.format(folder_pathway, each_file)
        grand_count += 1

        try:
            temp1_extractor = FirstInfoExtractor(filepath, df1)
            temp1_extractor.extract_and_add()
            df1 = temp1_extractor.update_df()

            count_one += 1
            del temp1_extractor
            gc.collect()
        except Exception as e:
            err_count_one += 1
            error_files_one.append(each_file)

        try:
            temp_mid_extractor = MidInfoExtractor(filepath, df2, df3, df5)
            temp_mid_extractor.extract_and_add()
            df2 = temp_mid_extractor.update_second_df()
            df3 = temp_mid_extractor.update_third_df()
            df5 = temp_mid_extractor.update_fifth_df()
            count_two += 1
            count_three += 1
            count_five += 1
            del temp_mid_extractor
            gc.collect()
        except ValueError as e:
            # Note here that if there is an error in extracting Folder 2 info, then Folder 3 and 5's info will not be
            # extracted either. This means that the true 'missing' information count for Folders 2, 3, 5 is the sum of
            # err_count_two, err_count_three and err_count_five. The union of error_files_two, error_files_three and
            # error_files_five is also all the files which did not have any information extracted for Folders 2, 3, 5.
            if e.args[0] == 2:
                err_count_two += 1
                error_files_two.append(each_file)
            elif e.args[0] == 3:
                err_count_three += 1
                error_files_three.append(each_file)
            elif e.args[0] == 5:
                err_count_five += 1
                error_files_five.append(each_file)

        try:
            temp4_extractor = FourthInfoExtractor(filepath, df4)
            temp4_extractor.extract_and_add()
            df4 = temp4_extractor.update_df()
            count_four += 1
            del temp4_extractor
            gc.collect()

        except Exception as e:
            err_count_four += 1
            error_files_four.append(each_file)

    if not os.path.exists('./Errors/FolderOneErrors'):
        os.makedirs('./Errors/FolderOneErrors', exist_ok=True)
    with open('./Errors/FolderOneErrors/error_list_one_{0}.txt'.format(file_suffix), 'w') as f:
        err_summ = 'Total files scanned: {0} | Number of error files: {1}'.format(count_one, err_count_one)
        f.write("%s\n" % err_summ)
        for err in error_files_one:
            f.write("%s\n" % err)

    if not os.path.exists('./Errors/FolderTwoErrors'):
        os.makedirs('./Errors/FolderTwoErrors', exist_ok=True)
    with open('./Errors/FolderTwoErrors/error_list_two_{0}.txt'.format(file_suffix), 'w') as f:
        err_summ = 'Total files scanned: {0} | Number of error files: {1}'.format(count_two, err_count_two)
        f.write("%s\n" % err_summ)
        for err in error_files_two:
            f.write("%s\n" % err)

    if not os.path.exists('./Errors/FolderThreeErrors'):
        os.makedirs('./Errors/FolderThreeErrors', exist_ok=True)
    with open('./Errors/FolderThreeErrors/error_list_three_{0}.txt'.format(file_suffix), 'w') as f:
        err_summ = 'Total files scanned: {0} | Number of error files: {1}'.format(count_three, err_count_three)
        f.write("%s\n" % err_summ)
        for err in error_files_three:
            f.write("%s\n" % err)

    if not os.path.exists('./Errors/FolderFourErrors'):
        os.makedirs('./Errors/FolderFourErrors', exist_ok=True)
    with open('./Errors/FolderFourErrors/error_list_four_{0}.txt'.format(file_suffix), 'w') as f:
        err_summ = 'Total files scanned: {0} | Number of error files: {1}'.format(count_four, err_count_four)
        f.write("%s\n" % err_summ)
        for err in error_files_four:
            f.write("%s\n" % err)

    if not os.path.exists('./Errors/FolderFiveErrors'):
        os.makedirs('./Errors/FolderFiveErrors', exist_ok=True)
    with open('./Errors/FolderFiveErrors/error_list_five_{0}.txt'.format(file_suffix), 'w') as f:
        err_summ = 'Total files scanned: {0} | Number of error files: {1}'.format(count_five, err_count_five)
        f.write("%s\n" % err_summ)
        for err in error_files_five:
            f.write("%s\n" % err)

    final_df1 = pd.DataFrame(df1)
    out_dir1 = './Folder1'
    if not os.path.exists(out_dir1):
        os.makedirs(out_dir1, exist_ok=True)
    result_name = '{0}/File_1_{1}.csv'.format(out_dir1, file_suffix)
    final_df1.to_csv(path_or_buf=result_name, index=False)
    del final_df1, df1
    gc.collect()

    final_df2 = pd.DataFrame(df2)
    out_dir2 = './Folder2'
    if not os.path.exists(out_dir2):
        os.makedirs(out_dir2, exist_ok=True)
    result_name = '{0}/File_2_{1}.csv'.format(out_dir2, file_suffix)
    final_df2.to_csv(path_or_buf=result_name, index=False)
    del final_df2, df2
    gc.collect()

    final_df3 = pd.DataFrame(df3)
    out_dir3 = './Folder3'
    if not os.path.exists(out_dir3):
        os.makedirs(out_dir3, exist_ok=True)
    result_name = '{0}/File_3_{1}.csv'.format(out_dir3, file_suffix)
    final_df3.to_csv(path_or_buf=result_name, index=False)
    del final_df3, df3
    gc.collect()

    final_df4 = pd.DataFrame(df4)
    out_dir4 = './Folder4'
    if not os.path.exists(out_dir4):
        os.makedirs(out_dir4, exist_ok=True)
    result_name = '{0}/File_4_{1}.csv'.format(out_dir4, file_suffix)
    final_df4.to_csv(path_or_buf=result_name, index=False)
    del final_df4, df4
    gc.collect()

    final_df5 = pd.DataFrame(df5)
    out_dir5 = './Folder5'
    if not os.path.exists(out_dir5):
        os.makedirs(out_dir5, exist_ok=True)
    result_name = '{0}/File_5_{1}.csv'.format(out_dir5, file_suffix)
    final_df5.to_csv(path_or_buf=result_name, index=False)
    del final_df5, df5
    gc.collect()

    print(
        'Total files cleared: {0} | Number of error files (for folders 2, 3, 5): {1} | Percentage = {2:.2f}%'.format(
        grand_count, err_count_two+err_count_three+err_count_five, ((err_count_two+err_count_three+err_count_five)/grand_count)*100.0
        )
    )

if __name__ == "__main__":
    main(ast.literal_eval(sys.argv[2]), sys.argv[3], sys.argv[1])
