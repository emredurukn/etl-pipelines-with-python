import os, time
import pandas as pd


def find_gcp_users(project_name):

    user_list = []
    file_name = "{}-users.txt".format(project_name)
    
    os.system("rm {}".format(file_name))

    os.system("gcloud projects get-iam-policy {} >> {}".format(project_name, file_name))

    with open(file_name, "r+") as f:
        for row in f.readlines():
            if "user:" in row:
                user_list.append(row.split("user:")[1].replace("\n", ""))

    os.system("rm {}".format(file_name))
    
    user_list = list(set(user_list))
    print("user_list length: {}".format(len(user_list)))
    
    return user_list


def create_bq_dataset_permission_file(project_name, bq_dataset_permission_file_name, user_list):

    bq_dataset_permission_list = []
    file_name = "user_permission.txt"

    for user_mail in user_list:
        project_permission = False
        
        result = os.system("rm {}".format(file_name))
        
        find_permission_command = """
        gcloud asset search-all-iam-policies --scope=projects/{} --query="policy:{}" >> {}
        """.format(project_name, user_mail, file_name)
        
        result = os.system(find_permission_command)
    
        user_datasets = []
        content = ""
        
        with open(file_name, "r+") as f:
            content = f.read()
    
        for part in content.split("assetType: cloudresourcemanager.googleapis.com/Project")[1:]:
            for role_part in part.split("role: roles/")[1:]:
                role = role_part.split("\n")[0]
                
                if role in ("viewer", "editor", "owner", "bigquery.dataViewer", "bigquery.dataEditor", "bigquery.admin"):
                    project_permission = True
                    break
    
        if project_permission == False:
            for part in content.split("assetType: bigquery.googleapis.com/Dataset")[1:]:
                user_datasets.append(part.split("\n---")[0].split("bigquery.googleapis.com/projects/{}/datasets/".format(project_name))[1])
        
            for dataset_name in user_datasets:
                bq_dataset_permission_list.append({
                    "user_mail": user_mail,
                    "dataset_name": dataset_name
                })
        else:
            bq_dataset_permission_list.append({
                    "user_mail": user_mail,
                    "dataset_name": "all"
            })

    result = os.system("rm {}".format(file_name))

    bq_dataset_permission_df = pd.DataFrame(bq_dataset_permission_list)
    bq_dataset_permission_df.to_csv(bq_dataset_permission_file_name, index=False)

    print("{} created".format(bq_dataset_permission_file_name))
    print("bq_dataset_permission length: {}".format(len(bq_dataset_permission_df)))


if __name__ == "__main__":

    project_name = "GCP_PROJECT_NAME"
    bq_dataset_permission_file_name = "bq_dataset_permissions.csv"

    start_time = time.time()

    user_list = find_gcp_users(project_name)
    create_bq_dataset_permission_file(project_name, bq_dataset_permission_file_name, user_list)

    end_time = time.time()

    print("process_time: {}".format((end_time - start_time)))
    print("-------------------------------")
