#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
This is public utility script with functions needed to realize execution of complex pipelines
on Kaggle platform. Example of usage is in notebook 
https://www.kaggle.com/code/svendaj/running-pipeline-of-notebooks-with-kaggle-api

Script needs to be added from netebook `File > Add utility script` menu and 
then it can be imported as other modules by `import pipex`

Kaggle API needs to be authenticated before calling the module functions.

Pushing notebook for execution is always "asynchronous" - it just uploads the notebook and returns back. 
So main difference between sequential and parallel execution is in waiting for completion of notebook run. 
Parallel execution means in fact asynchronous waiting for completion of notebook. For this purpose we will 
utilise `asyncio` library. Also because `time.sleep()` blocks the whole thread we will 
be using `asyncio.sleep()` function instead.

Version 5 2024/03/20 - adds treatment of internal server error during the push of notebook and waiting for completion
Version 6 2024/10/13 - limits printout of PUSH_ERROR only to first push attempt
"""
# imports
import asyncio, subprocess, os, json
from datetime import datetime

# get current working directory where subfolders will be created
wrk_dir = os.getcwd()

# function definitions

# remove_prefix is helper function removing prefix string from list of strings.
def remove_prefix(list_of_strings: list, prefix: str) -> list:
    """
    Function goes thru list of strings and removes prefix from string
    and returns list of strings without prefix
    
    Parameters
    ----------
    list_of_strings: list
        input list of strings

    prefix: string
        string to be removed from beginning of each list member if present
        if prefix is not present, list member is not modified
    
    Returns
    -------
        list of strings with removed prefix    
    """
    return [item[len(prefix):] if item.startswith(prefix) 
            else item for item in list_of_strings]

async def push_ntbk(ntbk_ref: str, DELAY: int = 60) -> None:
    """
    Function takes notebook reference in the form of username/notebook-name 
    creates working directory into which the metadata will be downloaded and then uploaded back for execution. 
    In case there are no available CPUs, function delays execution for DELAY seconds and then retries upload.
    
    Parameters
    ----------
    ntbk_ref: string
        notebook reference in the form of 'username/notebook-name' to be pushed
        for execution to Kaggle platform

    DELAY: integer, optional(default=60)
        number of seconds to wait before next push attempt
    
    Returns
    -------
        nothing    

    """
    # create folder for notebook download (pull) and upload for execution (push)
    ntbk_name = ntbk_ref.split("/")[1]
    krnl_mtdt_dir = os.path.join(wrk_dir, ntbk_name)
    while os.path.exists(krnl_mtdt_dir):
        krnl_mtdt_dir = os.path.join(krnl_mtdt_dir, ntbk_name)
    os.mkdir(krnl_mtdt_dir)

    # download notebook and generate metadata JSON file used for upload for execution
    response = subprocess.run(["kaggle", "kernels", "pull", "-p", 
                               krnl_mtdt_dir, "-m", ntbk_ref], capture_output=True)

    # Kaggle API contains bug which is causing failure of pushed notebooks in case it uses resources from other 
    # datasets or notebooks (most probably also competitions, but I was not able to confirm), 
    # because generated metadata contains resources references in wrong format. The bug needs to be corrected before pushing.
    krnl_mtdt_file = os.path.join(krnl_mtdt_dir, "kernel-metadata.json")
    orig_krnl_mtdt_file = os.path.join(krnl_mtdt_dir, "kernel-metadata.json.old")
    if not os.path.exists(krnl_mtdt_file):
        print(datetime.now(), "No metadata file found. Kaggle CLI output:", 
              response.stdout.decode("utf-8"))
        print("Ignoring notebook:", ntbk_ref)
        return
    os.rename(krnl_mtdt_file, orig_krnl_mtdt_file) 
    with open(orig_krnl_mtdt_file, "r") as file:
        metadata = json.load(file)
    # dataset resources had wrong 'datasets/' prefix in their references
    # notebook resources had wrong 'code/' prefix in their references
    metadata["dataset_sources"]  = remove_prefix(metadata["dataset_sources"], "datasets/")
    metadata["kernel_sources"]  = remove_prefix(metadata["kernel_sources"], "code/")
    # some notebooks had enable_internet setting changed to false
    # I was not able to identify why, so I am enabling internet in all notebooks
    metadata["enable_internet"] = True
    # save corrected metadata back before pushing it for execution
    with open(krnl_mtdt_file, "w") as file:
        metadata = json.dump(metadata, file)

    # upload corrected notebook for execution in while cycle waiting for free CPUs
    # in case of over limit requests to run, retry push after DELAY of seconds
    # hoping that other running notebooks completed and freed CPU resources
    wait_for_CPU_free = True
    err_report = False
    while wait_for_CPU_free:
        # attempt push
        response = subprocess.run(["kaggle", "kernels", "push", "-p", krnl_mtdt_dir], 
                                  capture_output=True)
        resp_text = response.stdout.decode("utf-8")
        PUSH_ERR = "push error: Maximum batch CPU" # push error substring indicating no free CPU
        INT_SERV_ERROR = "500 - Internal Server Error" 
        if (PUSH_ERR in resp_text) or (INT_SERV_ERROR in resp_text): 
            # if there would be other push or internal server error
            # push would be considered as successfull
            if not err_report:
                print(datetime.now(), ntbk_ref, resp_text)
                err_report = True
            await asyncio.sleep(DELAY)
        else:
            # push went thru without CPU limit reached message, so it is assumed as successful
            # not very good checking of response for other problems, pushed notebook might fail
            print(datetime.now(), "Assuming", ntbk_ref, "successfull push: ", resp_text)
            wait_for_CPU_free = False

async def wait_for_ntbk_completion(ntbk_ref: str, DELAY: int = 60) -> None:
    """
    Function takes notebook reference in the form of username/notebook-name 
    and checks status of notebook execution. If status is unchanged from previous check, 
    wait DELAY seconds before next check until status is 'complete'.
    
    Parameters
    ----------
    ntbk_ref: string
        notebook reference in the form of 'username/notebook-name' to be checked

    DELAY: integer, optional(default=60)
        number of seconds to wait before next check
    
    Returns
    -------
        nothing    

    """
    # until status of notebook is "complete" stay in while loop
    # if status is not changed, wait for DELAY seconds
    status = ""
    while status != "complete":
        response = subprocess.run(["kaggle", "kernels", "status", ntbk_ref], capture_output=True)
        resp_text = response.stdout.decode("utf-8")
        if resp_text.startswith("403 - Forbidden"):
            print("Notebook", ntbk_ref, " probably does not exists. Response of Kaggle API CLI:", resp_text)
            return
        elif resp_text.startswith("500 - Internal Server Error"):
            new_status = "ServerError"
        else:
            new_status = resp_text.split('"')[1]  # status test is between ""
        if status != new_status:
            print(datetime.now(), resp_text)
            status = new_status
        else:
            await asyncio.sleep(DELAY)
            

async def execute_pipeline(pipeline: dict, DELAY: int = 60) -> None:
    """
    Recursive function which goes thru dictionary describing desired notebook execution pipeline. 
    Each pipeline is list of individual notebooks or other pipelines with attribute specifying 
    if the list should be executed sequentially (await runs coroutine and waits on its return) or 
    in parallel (asyncio Task is send for execution and returns back without waithing for completion). 
    Parallel execution then waits for completion of all launched tasks. If there are still some tasks running, 
    waiting is postponed for DELAY seconds and checks again.
    
    Parameters
    ----------
    pipeline: dictionary
        pipeline = {
              "list" = [list of strings (notebook references) or other pipelines],
              "execution" = "sequential" or "parallel"
              }

    DELAY: integer, optional(default=60)
        number of seconds to wait for check of pipeline completion 
    
    Returns
    -------
        nothing    

    """
    # recursive function going thru pipeline and executing it
    if pipeline["execution"] == 'sequential':
        for segment in pipeline["list"]:
            if type(segment) == dict:
                await execute_pipeline(segment)
            else:  # must be string with ntbk name 
                await push_ntbk(segment)
                await wait_for_ntbk_completion(segment)
    
    else:  # parallel execution
        exec_queue = []    # list with all tasks send asychronously for execution
        for segment in pipeline["list"]:
            if type(segment) == dict:
                # tasks created for async execution are appended to list for later control
                exec_queue.append(asyncio.create_task(execute_pipeline(segment)))
            else:  # must be string with ntbk name 
                await push_ntbk(segment)  # no need for Task creation as push operation is quick
                exec_queue.append(asyncio.create_task(wait_for_ntbk_completion(segment)))

        # wait for all launched tasks completion
        tasks_running = True
        #display(exec_queue)
        while tasks_running:
            tasks_running = any([not x.done() for x in exec_queue])
            if tasks_running:
                await asyncio.sleep(DELAY)

def is_valid_pipeline(pipeline):
    """
    Recursive function checking validity of pipeline structure (required attributes and its value types) 
    so that it will not cause problems during execution. It is not checking if notebooks are existing, 
    which would not cause execution problems, because such notebook names would be ignored.
    
    Parameters
    ----------
    pipeline: dictionary
        pipeline = {
              'list' = [list of strings (notebook references) or other pipelines],
              'execution' = 'sequential' or 'parallel'
              }

    Returns
    -------
        bool
            True if provided pipeline structure is valid (list is list of strings or other pipelines, 
            execution is 'sequential' or 'parallel'). False otherwise.
    """
    # walks thru pipeline and checks structure of pipeline 
    required_keys = ["list", "execution"]
    allowed_exec_values = ["sequential", "parallel"]
    if required_keys[0] in pipeline.keys() and required_keys[1] in pipeline.keys():
        if type(pipeline[required_keys[0]]) != list:
            print("Pipeline's list value is not a list:", pipeline[required_keys[0]])
            return False
        if  not (pipeline[required_keys[1]] in allowed_exec_values):
            print("Pipeline's execution value is not allowed:", pipeline[required_keys[1]])
            return False
        for item in pipeline[required_keys[0]]:
            if type(item) != str:
                if type(item) == dict and not is_valid_pipeline(item):
                    print("Sub pipeline is not valid:", item)
                    return False
            # if it is the string not checking if it is valid notebook
        return True
    else:
        print(f"Pipeline has not required attributes '{required_keys[0]}' and '{required_keys[1]}'")
        return False


# In[ ]:


