from DIRAC import S_ERROR, S_OK

BASH_TEMPLATE = """\
#!/bin/bash
BASEDIR=${{PWD}}
INPUT={inputs}
BUNDLE_ID={bundleId}

get_id() {{
    basename ${{1}} _workloadExec.sh
}}

run_task() {{
    local task_id=$(get_id $1)
    local input=${{1#${{task_id}}_*}}

    cd "$task_id"

    echo "[${{task_id}}] Executing task"

    # 'set -e' inside the job execution to obtain the real exit status in case of failure
    bash -e ${{input}} \\
        1> >(tee ${{BUNDLE_ID}}.out) \\
        2> >(tee ${{BUNDLE_ID}}.err 1>&2) &

    local task_pid=$!

    echo "[${{task_id}}] Waiting for pid ${{task_pid}}..."

    wait ${{task_pid}}
    local task_status=$?

    # Report status
    echo "[${{task_id}}] ${{task_pid}} ${{task_status}}" | tee ${{BASEDIR}}/${{task_id}}.status
}}

# execute tasks
for input in ${{INPUT[@]}}; do
    [ -f "$input" ] || break

    jobId=$(get_id ${{input}})
    mkdir ${{jobId}}
    
    for filename in ${{jobId}}*; do
        [ -f ${{filename}} ] || continue
        touch ${{jobId}}.status
        # Move the job specific files to its directory, removing the jobId from its name
        mv $filename ${{jobId}}/${{filename#${{jobId}}_*}}
    done

    run_task ${{input}} &
done

# wait for all tasks
wait

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
