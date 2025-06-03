from DIRAC import S_ERROR, S_OK

GENERIC_BASH_TEMPLATE = """\
#!/bin/bash
set -e

BASEDIR=${{PWD}}
INPUT={inputs}

get_id() {{
    basename ${{1}} .json
}}

run_task() {{
    local input=$1
    local task_id=$(get_id ${{input}})

    >&2 echo "Executing task ${{task_id}}"
    >&2 {command} ${{BASEDIR}}/${{input}} >task_${{task_id}}.log 2>&1  &
    local task_pid=$!

    >&2 echo "Task ${{task_id}} waiting for pid ${{task_pid}}..."
    wait ${{task_pid}} ; local task_status=$?

    # report status
    echo "${{task_id}} ${{task_pid}} ${{task_status}}" | tee task_${{task_id}}.status
}}

# execute tasks
for input in ${{INPUT}}; do
    [ -f "$input" ] || break
    taskdir="task_$(get_id ${{input}})"
    mkdir ${{taskdir}} && cd "$_" &&
        run_task ${{input}} >> ${{BASEDIR}}/tasks_status.log &
    cd ${{BASEDIR}}
done

# wait for all tasks
wait
"""

def generate_template(template: str, inputs: list):
    template = template.lower().replace("-", "_")
    func_name = "_generate_" + template
    generator = locals()[func_name]

    if not generator:
        return S_ERROR("Template not found")

    result = generator(inputs)
    if not result["OK"]:
        return result
    
    return S_OK(result["Value"])

def _generate_lb_prod_run(inputs: list):
    template = __generate_generic_bash("lb-prod-run", inputs)
    return S_OK(template)

def _generate_bash(inputs: list):
    template = __generate_generic_bash("bash", inputs)
    return S_OK(template)

def __generate_generic_bash(command, inputs):
    formatted_inputs = "(" + ", ".join(inputs) + ")"
    template = GENERIC_BASH_TEMPLATE.format(command=command, inputs=formatted_inputs)
    return template
