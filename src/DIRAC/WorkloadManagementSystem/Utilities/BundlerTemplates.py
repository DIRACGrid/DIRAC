from DIRAC import S_ERROR, S_OK

BASH_TEMPLATE = """\
#!/bin/bash
BASEDIR=${{PWD}}
INPUT={inputs}
BUNDLE_ID={bundleId}

monitor_job() {{
    local job_pid=$1

    #First time with headers
    ps -p "$job_pid" -o pid,psr,%cpu,%mem,time,wchan,class,vsz,drs,rss,uss,size,rops,wops,wbytes

    while : ; do
        sleep 5

        # If the job finished, kill the monitoring        
        if ! kill -0 "$job_pid" 2>/dev/null; then
            break  
        fi

        ps -p -h "$job_pid" -o pid,psr,%cpu,%mem,time,wchan,class,vsz,drs,rss,uss,size,rops,wops,wbytes
    done
}}

get_id() {{
    echo $1 | cut -d '_' -f 1
}}

run_task() {{
    local task_id=$(get_id $1)
    local input=${{1#${{task_id}}_*}}

    cd "$task_id"

    echo "[${{task_id}}] Executing task"

    # 'set -e' inside the job execution to obtain the real exit status in case of failure
    bash -e ${{input}} \\
        1> >(tee ${{BUNDLE_ID}}.out) \\
        2> >(tee ${{BUNDLE_ID}}.err 1>&2)

    local task_status=$?

    # Report job ending and status
    echo "[${{task_id}}] Task Finished"
    echo "${{task_status}}" 1>${{BASEDIR}}/${{task_id}}.status
    echo "[${{task_id}}] Process final status: ${{task_status}}"
}}

# execute tasks
for input in ${{INPUT[@]}}; do
    [ -f "$input" ] || break

    local jobId=$(get_id ${{input}})
    mkdir ${{jobId}}
    
    for filename in ${{jobId}}*; do
        [ -f ${{filename}} ] || continue
        touch ${{jobId}}.status
        # Move the job specific files to its directory, removing the jobId from its name
        mv $filename ${{jobId}}/${{filename#${{jobId}}_*}}
    done

    run_task ${{input}} &
    pid=$!
    pids+=($pid)

    monitor_job "$pid" > ${{jobId}}/monitoring.stats &
done

# wait for all tasks
wait "${{pids[@]}}"

# Checksum of all files in the root and the job subdirectories
find -H ! -type d ! -name md5Checksum.txt -exec md5sum {{}} + >md5Checksum.txt
"""


def generate_template(template: str, inputs: list, bundleId: str):
    template = template.lower().replace("-", "_")
    func_name = "_generate_" + template
    generator = globals()[func_name]

    if not generator:
        return S_ERROR("Template not found")

    if inputs is None:
        inputs = []

    template, formatMap = generator(inputs)
    formatMap["bundleId"] = bundleId

    return S_OK(template.format(**formatMap))


def _generate_bash(inputs: list):
    formatted_inputs = "(" + " ".join(inputs) + ")"
    formatMap = {"inputs": formatted_inputs}
    return BASH_TEMPLATE, formatMap
