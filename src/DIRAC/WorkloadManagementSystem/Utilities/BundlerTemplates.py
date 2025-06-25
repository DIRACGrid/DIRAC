from DIRAC import S_ERROR, S_OK

GENERIC_BASH_TEMPLATE = """\
#!/bin/bash
set -e

BASEDIR=${{PWD}}
INPUT={inputs}

get_id() {{
    basename ${{1}} _workloadExec.sh
}}

run_task() {{
    local input=$1
    local task_id=$(get_id ${{input}})

    # Setup
    touch ${{task_id}}.status
    touch ${{task_id}}.out

    echo "Executing task ${{task_id}}"
    {command} ${{BASEDIR}}/${{input}} >${{task_id}}.out 2>&1  &
    local task_pid=$!

    echo "Task ${{task_id}} waiting for pid ${{task_pid}}..."
    wait ${{task_pid}} ; local task_status=$?

    # Report status
    echo "${{task_id}} ${{task_pid}} ${{task_status}}" | tee ${{task_id}}.status
}}

# execute tasks
for input in ${{INPUT[@]}}; do
    [ -f "$input" ] || break
    run_task ${{input}} &
done

# wait for all tasks
wait
"""


def generate_template(template: str, inputs: list):
    template = template.lower().replace("-", "_")
    func_name = "_generate_" + template
    generator = globals()[func_name]

    if not generator:
        return S_ERROR("Template not found")

    if inputs is None:
        inputs = []

    return generator(inputs)


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
