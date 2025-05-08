from DIRAC import S_ERROR, S_OK


def generate_template(template: str, inputs: list[str]):
    template_lower = template.lower()
    func_name = "_generate_" + template_lower
    generator = globals()[func_name]
    
    if not generator:
        return S_ERROR("Template not found")
    
    template, formatted_inputs = generator(inputs)

    return S_OK(template.format(inputs=formatted_inputs))

def _generate_bash(inputs: list[str]):
    template = """\
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
    >&2 bash ${{BASEDIR}}/${{input}} >task_${{task_id}}.log 2>&1  &
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

    formatted_inputs = '(' + ', '.join(inputs) + ')'

    return template, formatted_inputs
