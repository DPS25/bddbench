
## 0. Prerequisites

Before you start, please ensure:

* OS: A Linux distribution that supports `systemd --user`
* Installed tools:

  * `git`
  * `bash`
  * Python and the project’s Python dependencies (see the project’s dependency docs)
* Your InfluxDB instance is up and reachable, and you know:

  * `INFLUX_URL`
  * `INFLUX_ORG`
  * `INFLUX_BUCKET`
  * `INFLUX_TOKEN`

---

## 1. Clone the repository

```bash
git clone https://github.com/Dopafish/bddbench.git
cd bddbench
```

---

## 2. Configure Influx environment variables (critical)

Before the first run, you **must** adjust the Influx configuration in the script and the env file to match your own server.

### 2.1 Edit `gherkin-poc/scripts/nightly_run.sh`

Open the script:

```bash
vim gherkin-poc/scripts/nightly_run.sh
```

Find a block like:

```bash
export INFLUX_URL="http://localhost:8086"
export INFLUX_ORG="3S"
export INFLUX_BUCKET="dsp25"
export INFLUX_TOKEN="YOUR_INFLUX_TOKEN_HERE"
```

Replace the four variables with your real settings, e.g.:

```bash
export INFLUX_URL="http://your-influx-host:8086"
export INFLUX_ORG="your-org"
export INFLUX_BUCKET="your-bucket"
export INFLUX_TOKEN="your-real-token"
```

> Tip: Keep real tokens only on the server; do not commit them back to the repository.

---

### 2.2 Edit `gherkin-poc/systemd/bddbench-nightly.env`

Open the env template:

```bash
vim gherkin-poc/systemd/bddbench-nightly.env
```

You will see something like:

```bash
INFLUX_URL=http://localhost:8086
INFLUX_ORG=3S
INFLUX_BUCKET=dsp25
INFLUX_TOKEN=YOUR_INFLUX_TOKEN_HERE
```

Replace these with your own Influx configuration, e.g.:

```bash
INFLUX_URL=http://your-influx-host:8086
INFLUX_ORG=your-org
INFLUX_BUCKET=your-bucket
INFLUX_TOKEN=your-real-token
```

> This file will later be copied to `~/.config/bddbench-nightly.env` and used by the systemd service.

---

## 3. Make the scripts executable (one-time)

From the project root, run:

```bash
chmod +x gherkin-poc/scripts/nightly_run.sh
chmod +x gherkin-poc/systemd/install_bddbench_user_units.sh
```

> If you’re not sure whether you did this before, running `chmod +x` again is harmless.

---

## 4. Run the nightly script manually once (recommended)

Before delegating everything to the timer, it’s a good idea to run the script once manually to verify your environment and Influx configuration:

```bash
./gherkin-poc/scripts/nightly_run.sh
```

### 4.1 What does this script do?

* Runs the relevant tests (e.g. gherkin/cucumber scenarios, t-test analysis, etc.)
* Produces performance/result plots

The generated images are stored in:

```text
gherkin-poc/reports/plots/
```

Characteristics:

* Each run produces **4 images**
* Each image filename contains a **timestamp**, so you can easily distinguish different runs

If the script finishes successfully and you see newly created timestamped images in `gherkin-poc/reports/plots/`, you are ready for the next step.

---

## 5. Install the systemd user timer

When manual runs work, you can attach the nightly job to `systemd --user` using the install script:

```bash
./gherkin-poc/systemd/install_bddbench_user_units.sh
```

### 5.1 What does this script do to your system?

The script performs the following actions:

1. **Create the user-level systemd directory (if needed)**

   ```bash
   mkdir -p "$HOME/.config/systemd/user"
   ```

2. **Copy unit files into the user directory**

   ```bash
   cp gherkin-poc/systemd/bddbench-nightly.service $HOME/.config/systemd/user/
   cp gherkin-poc/systemd/bddbench-nightly.timer   $HOME/.config/systemd/user/
   ```

   So they end up at:

   ```text
   ~/.config/systemd/user/bddbench-nightly.service
   ~/.config/systemd/user/bddbench-nightly.timer
   ```

3. **Prepare the env file**

   * If `$HOME/.config/bddbench-nightly.env` does **not** exist:

     * It copies your edited `gherkin-poc/systemd/bddbench-nightly.env` to that location.
   * If `$HOME/.config/bddbench-nightly.env` already exists:

     * It keeps the existing file and does not override it.

4. **Reload the user-level systemd configuration**

   ```bash
   systemctl --user daemon-reload
   ```

5. **Enable and start the timer**

   ```bash
   systemctl --user enable --now bddbench-nightly.timer
   ```

   This makes sure `bddbench-nightly.timer` is started and will trigger according to the `OnCalendar=` rule defined in the `.timer` file.

6. **Run a sanity-check service invocation**

   ```bash
   systemctl --user start --wait bddbench-nightly.service
   ```

   * On success: prints `[test] Service ran successfully.`
   * On failure: prints `systemctl --user status` and the tail of `journalctl --user -xeu bddbench-nightly.service` to help you debug.

> Note: All of this is **user-level** only (`~/.config` + `systemctl --user`), it does **not** touch global `/etc/systemd/system`.

---

## 6. Adjust the schedule (optional)

The schedule is defined in `bddbench-nightly.timer`. You can change it to fit your needs.

Edit:

```bash
vim gherkin-poc/systemd/bddbench-nightly.timer
```

Look for the `OnCalendar=` line in the `[Timer]` section, for example:

```ini
[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true
```

Examples:

* Run daily at 01:00:

  ```ini
  OnCalendar=*-*-* 01:00:00
  ```

* Run every hour:

  ```ini
  OnCalendar=hourly
  ```

After modifying the `.timer` (or `.service` / `.env`), you **only need to rerun the install script once**:

```bash
./gherkin-poc/systemd/install_bddbench_user_units.sh
```

The script will:

* Copy the updated service/timer to the user directory
* Reload systemd
* Re-enable and restart `bddbench-nightly.timer`
* Run the sanity check again

---

## 7. Check status, logs and output plots

### 7.1 Check timer and service status

```bash
systemctl --user status bddbench-nightly.timer
systemctl --user status bddbench-nightly.service
```

### 7.2 View recent logs

```bash
journalctl --user -u bddbench-nightly.service --since "1 hour ago"
```

### 7.3 View generated images

All nightly plots are stored in:

```text
gherkin-poc/reports/plots/
```

* Each run produces 4 images
* Filenames contain timestamps (for easy comparison across runs/days)

You can use these plots for:

* Reports and presentations
* Internal dashboards / wikis
* Comparing performance across different versions or configurations


