BASH_WRAPPER = """\
#!/bin/bash
BASEDIR=${{PWD}}
INPUT={inputs}
BUNDLE_ID={bundleId}

get_id() {{
    echo $1 | cut -d '_' -f 1
}}

job_number=0
chmod u+x run_task.sh

# execute tasks
for input in ${{INPUT[@]}}; do
    [ -f "$input" ] || break

    jobId=$(get_id ${{input}})
    mkdir ${{jobId}}

    for filename in ${{jobId}}*; do
        [ -f ${{filename}} ] || continue
        # Move the job specific files to its directory, removing the jobId from its name
        mv $filename ${{jobId}}/${{filename#${{jobId}}_*}}
    done

    ${{BASEDIR}}/run_task.sh ${{jobId}} ${{input}} ${{BUNDLE_ID}} ${{BASEDIR}} &
done

# wait for all tasks
wait
"""

BASH_RUN_TASK = """\
#!/bin/bash
task_id=$1
input=${2#${task_id}_*}
bundle_id=$3
base_dir=$4

cd "$task_id"

echo "[${task_id}] Executing task"

# 'set -e' inside the job execution to obtain the real exit status in case of failure
bash -e ${input} \\
        1> ${bundle_id}.out \\
        2> ${bundle_id}.err

task_status=$?

# Report job ending and status
echo "[${task_id}] Task Finished"
echo "[${task_id}] Process final status: ${task_status}"
"""
