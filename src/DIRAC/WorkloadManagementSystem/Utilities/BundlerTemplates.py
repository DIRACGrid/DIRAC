from DIRAC import S_ERROR, S_OK

# DEPRECATED
BASH_TESTING_TEMPLATE = """\
#!/bin/bash
BASEDIR=${{PWD}}
INPUT={inputs}
BUNDLE_ID={bundleId}

OLD_IFS=$IFS

# cpu management
bundler_pid=$$
allowed_cpus=$(grep -w Cpus_allowed_list /proc/"$bundler_pid"/status | awk '{{print $2}}')
IFS=',' read -a cpu_ranges <<< "$allowed_cpus"

IFS=$OLD_IFS

first_allowed_cpu=$(cut -d "-" -f 1 - <<<"${{cpu_ranges[0]}}")
last_allowed_cpu=$(cut -d "-" -f 2 - <<<"${{cpu_ranges[-1]}}")
cpu_offset=0
total_allowed_cpus=0

calc_total_cpus() {{
    for range in "${{cpu_ranges[@]}}"; do
        local min=$(cut -d "-" -f 1 - <<<"$range")
        local max=$(cut -d "-" -f 2 - <<<"$range")
        total_allowed_cpus=$(($total_allowed_cpus+$max-$min+1))
    done
}}

next_allowed_cpu() {{
    echo $allowed_cpus
    return 0
     
    local desired_cpu=$(( ($1 + $cpu_offset) % $total_allowed_cpus ))
    local cpu=$first_allowed_cpu

    for range in "${{cpu_ranges[@]}}"; do
        local min=$(cut -d "-" -f 1 - <<<"$range")
        local max=$(cut -d "-" -f 2 - <<<"$range")
        local real_cpu=$(($min+$desired_cpu))
        
        if (( $real_cpu <= $max )); then
            cpu=$real_cpu
            break
        fi
    
        # Check next range
        local cpus_on_range=$(($max-$min+1))
        local desired_cpu=$(($desired_cpu-$cpus_on_range))
    done

    # Return cpu
    echo $cpu
}}

calc_total_cpus

echo This machine has "$total_allowed_cpus" valid cores
echo Ranges: "${{cpu_ranges[@]}}"

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

    cpu=$(next_allowed_cpu $job_number)
    taskset -c $cpu ${{BASEDIR}}/run_task.sh ${{jobId}} ${{input}} ${{BUNDLE_ID}} ${{BASEDIR}} &
    pid=$!

    pids+=($pid)
    job_number=$(($job_number+1))
done

# wait for all tasks
wait "${{pids[@]}}"
"""

BASH_TEMPLATE = """\
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
